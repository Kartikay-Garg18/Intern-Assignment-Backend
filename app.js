const express = require('express');
const cors = require('cors');
const { neon } = require('@neondatabase/serverless')
const path = require('path');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const dotenv = require('dotenv');

dotenv.config();

// Initialize Express app
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'dist')));

// Database connection
// const pool = new Pool({
//   connectionString: process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/data_agent',
// });
const pool = neon(process.env.DATABASE_URL)

// Initialize Gemini client
const gemini = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

// Cache for database schema
let dbSchemaCache = null;
let dbSchemaLastUpdated = null;
const SCHEMA_CACHE_TTL = 3600000; // 1 hour in milliseconds

// Utility to fetch and format database schema
async function getDatabaseSchema() {
  // Return cached schema if available and not expired
  if (dbSchemaCache && dbSchemaLastUpdated && (Date.now() - dbSchemaLastUpdated) < SCHEMA_CACHE_TTL) {
    return dbSchemaCache;
  }

  try {
    // Get all user tables in the current database's public schema
    const tablesResult = await pool`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
        AND table_name NOT LIKE 'pg_%'
        AND table_name NOT LIKE 'sql_%'
      ORDER BY table_name
    `;
    const tables = tablesResult.map(row => row.table_name);
    const schema = {};

    // For each table, get columns and sample data
    for (const table of tables) {
      // Get columns
      const columnsResult = await pool`
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = ${table}
        ORDER BY ordinal_position
      `;

      
      // Get primary key
      const pkResult = await pool`
      SELECT kcu.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = 'public'
          AND tc.table_name = ${table}
      `;
      
      // Get foreign keys
      const fkResult = await pool`
        SELECT
          kcu.column_name,
          ccu.table_name AS foreign_table_name,
          ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
          AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = 'public'
          AND tc.table_name = ${table}
      `;

      // Get sample data (first 5 rows)
      
      const query = `SELECT * FROM "${table}" LIMIT 5`;
      const sampleDataResult = await pool.query(query);

      schema[table] = {
        columns: columnsResult.map(col => ({
          name: col.column_name,
          type: col.data_type,
          nullable: col.is_nullable === 'YES'
        })),
        primaryKey: pkResult,
        foreignKeys: fkResult,
        sampleData: sampleDataResult
      };
    }

    // Get relationships between tables
    for (const table in schema) {
      const relationships = [];
      // Add relationships based on foreign keys
      for (const fk of schema[table].foreignKeys) {
        relationships.push({
          type: 'belongs_to',
          table: fk.references.table,
          foreignKey: fk.column,
          targetKey: fk.references.column
        });
      }
      // Find tables that reference this table
      for (const otherTable in schema) {
        if (otherTable === table) continue;
        for (const fk of schema[otherTable].foreignKeys) {
          if (fk.references.table === table) {
            relationships.push({
              type: 'has_many',
              table: otherTable,
              foreignKey: fk.column,
              targetKey: fk.references.column
            });
          }
        }
      }
      schema[table].relationships = relationships;
    }

    dbSchemaCache = schema;
    dbSchemaLastUpdated = Date.now();
    return schema;
  } catch (error) {
    console.error('Error fetching database schema:', error);
    throw error;
  }
}

// Infer column meanings and table relationships
async function inferSchemaSemantics(schema) {
  try {
    const columnPatterns = {
      id: /^id$|^.*_id$/i,
      name: /^name$|^.*_name$/i,
      date: /^date$|^.*_date$|^.*_at$/i,
      email: /^email$/i,
      price: /^price$|^.*_price$|^cost$|^.*_cost$/i,
      quantity: /^quantity$|^.*_quantity$|^count$|^.*_count$/i,
      status: /^status$|^.*_status$/i,
      type: /^type$|^.*_type$/i,
      description: /^description$|^.*_description$/i
    };

    const enhancedSchema = { ...schema };
    
    // Analyze each table
    for (const tableName in enhancedSchema) {
      const table = enhancedSchema[tableName];
      
      // Add semantic meanings to columns based on patterns
      table.columns = table.columns.map(col => {
        const semantics = {};
        
        // Check against patterns
        for (const [semantic, pattern] of Object.entries(columnPatterns)) {
          if (pattern.test(col.name)) {
            semantics.meaning = semantic;
            break;
          }
        }
        
        // Check data types for additional semantics
        if (col.type.includes('timestamp') || col.type.includes('date')) {
          semantics.temporal = true;
        } else if (col.type.includes('int') || col.type.includes('decimal') || col.type.includes('float')) {
          semantics.numeric = true;
          
          // Check for likely metrics based on name
          if (/amount|total|sum|price|cost|revenue|profit|count|quantity/i.test(col.name)) {
            semantics.metric = true;
          }
        }
        
        return {
          ...col,
          semantics
        };
      });
      
      // Infer table purpose based on columns and relationships
      const hasTimestamps = table.columns.some(col => 
        col.type.includes('timestamp') || col.type.includes('date')
      );
      
      const hasMetrics = table.columns.some(col => 
        col.semantics && col.semantics.metric
      );
      
      if (tableName.toLowerCase().includes('fact')) {
        table.semantics = { type: 'fact_table' };
      } else if (tableName.toLowerCase().includes('dim') || tableName.toLowerCase().includes('dimension')) {
        table.semantics = { type: 'dimension_table' };
      } else if (hasTimestamps && hasMetrics) {
        table.semantics = { type: 'transaction_or_event' };
      } else if (table.relationships.filter(r => r.type === 'has_many').length > 2) {
        table.semantics = { type: 'central_entity' };
      }
    }
    
    return enhancedSchema;
  } catch (error) {
    console.error('Error inferring schema semantics:', error);
    return schema; // Return original schema on error
  }
}

// Generate SQL from natural language query
async function generateSQL(question, schema) {
  try {
    const prompt = `
You are an expert SQL query generator.

**Instructions:**
- Return ONLY a valid SQL query as plain text, using PostgreSQL syntax. Do NOT include any explanation, description, comments, or formatting—just the SQL query.
- Use ONLY the tables and columns provided in the schema below.
- If the question cannot be answered exactly with the schema, generate any plausible, valid SQL query using the available tables/columns that is as relevant as possible to the question.
- Never return an error message, fallback string, or any text except a SQL query—always return a SQL query.
- All SQL queries must use PostgreSQL syntax and functions (e.g., use CURRENT_DATE - INTERVAL '3 months' for date math).
- Do NOT use SQLite, MySQL, or other dialects' functions like date('now', ...).
- If a date or timestamp comparison is needed and the column is of type text, cast it to DATE or TIMESTAMP in the SQL query (e.g., WHERE CAST(column AS DATE) >= ...).

Schema:
${JSON.stringify(schema, null, 2)}

Question:
${question}
`;

    const model = gemini.getGenerativeModel({ model: "gemini-1.5-flash" });
    const result = await model.generateContent(prompt);
    const response = await result.response;
    let sql = response.text().trim();

    // Remove Markdown code block markers if present
    sql = sql.replace(/```sql|```/gi, '').trim();

    // If a date or timestamp comparison is needed and the column is of type text, cast it to DATE or TIMESTAMP in the SQL query.

    return sql;
  } catch (error) {
    console.error('Error generating SQL with Gemini:', error);
    throw new Error('Failed to generate SQL query');
  }
}

// Execute SQL query
async function executeQuery(sql) {
  try {
    // Add safety timeout
    
    const result = await Promise.race([
      await pool.query(sql),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Query timeout after 30 seconds')), 30000)
      )
    ]);
    if (!result || !Array.isArray(result)) throw new Error('No result returned from query');
    return {
      columns: result.length > 0 ? Object.keys(result[0]) : [],
      rows: result
    };
  } catch (error) {
    console.error('Error executing query:', error);
    throw error;
  }
}

// Generate visualizations from query results
async function generateVisualizations(question, queryResults, sql) {
  try {
    const prompt = `
You are an expert data visualization specialist. Your task is to generate appropriate visualization specifications based on the SQL query results.

Guidelines:
- Analyze the data structure and the question to determine appropriate visualizations.
- Return JSON array of visualization specifications, each with these properties:
  - type: 'bar', 'line', 'pie', or 'table'
  - title: Descriptive title for the visualization
  - data: The processed data for the visualization
  - For bar/line charts: include xAxis, series (array of {dataKey, color})
  - For pie charts: include nameKey, valueKey, colors array
- Choose appropriate visualization types:
  - Bar charts for comparisons across categories
  - Line charts for trends over time
  - Pie charts for composition/proportion analysis (limit to 7 segments max)
  - Tables for detailed data or when other visualizations aren't appropriate
- Process the data appropriately for each visualization type
- Limit to a maximum of 2 visualizations unless more are clearly needed
- Ensure data is properly formatted for the visualization library (Recharts)

The original question was: "${question}"
The SQL query used was: ${sql}
The query results are: ${JSON.stringify(queryResults)}
`;

    const model = gemini.getGenerativeModel({ model: "gemini-1.5-flash" });
    const result = await model.generateContent(prompt);
    const response = await result.response;
    const content = response.text().trim();

    let visualizations = [];
    try {
      const jsonMatch = content.match(/```(?:json)?\s*([\s\S]*?)\s*```/) ||
                        content.match(/\[([\s\S]*?)\]/) ||
                        [null, content];

      const jsonStr = jsonMatch[1].startsWith('[') ? jsonMatch[1] : `[${jsonMatch[1]}]`;
      visualizations = JSON.parse(jsonStr);
    } catch (parseError) {
      console.error('Error parsing visualization JSON:', parseError);
      console.log('Raw content:', content);
      visualizations = [];
    }

    return visualizations;
  } catch (error) {
    console.error('Error generating visualizations with Gemini:', error);
    return [];
  }
}

// Generate natural language explanation of results
async function generateExplanation(query, queryResults, sql, visualizations) {
  try {
    const prompt = `
You are an expert business data analyst. Your task is to provide a natural language explanation of SQL query results in response to a business question.

Guidelines:
- Use clear, concise business language.
- Highlight key insights and patterns in the data.
- Provide context and interpretation beyond just describing the numbers.
- Connect the findings to potential business implications.
- Include specific numbers and percentages when relevant.
- Keep the tone professional but conversational.
- Be decisive in your analysis - don't hedge unnecessarily.
- Structure your response with a clear introduction, key findings, and conclusion.
- Limit your response to 3-5 paragraphs at most.
- Do not mention the SQL query itself.

The original question was: "${query}"
The SQL query used was: ${sql}
The query results are: ${JSON.stringify(queryResults)}
The visualizations are: ${JSON.stringify(visualizations)}
`;

    const model = gemini.getGenerativeModel({ model: "gemini-1.5-flash" });
    const result = await model.generateContent(prompt);
    const response = await result.response;
    return response.text().trim();
  } catch (error) {
    console.error('Error generating explanation with Gemini:', error);
    return "I couldn't generate an explanation for these results. Please review the data directly.";
  }
}

// API endpoint for natural language query
app.post('/api/query', async (req, res) => {
  const { query, history } = req.body;
  
  try {
    // Step 1: Get database schema
    const rawSchema = await getDatabaseSchema();
    const schema = await inferSchemaSemantics(rawSchema);
    
    // Step 2: Generate SQL from natural language
    const sql = await generateSQL(query, schema);
    
    // Step 3: Execute SQL query
    const queryResults = await executeQuery(sql);
    
    // Step 4: Generate visualizations
    const visualizations = await generateVisualizations(query, queryResults, sql);
    
    // Step 5: Generate natural language explanation
    const explanation = await generateExplanation(query, queryResults, sql, visualizations);
    
    // Send response
    res.json({
      text: explanation,
      sqlQuery: sql,
      tableData: queryResults,
      visualizations
    });
  } catch (error) {
    console.error('Error processing query:', error);
    res.status(500).json({ error: error.message });
  }
});

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date() });
});

// Serve React app for all other routes (must be last)
app.use((req, res, next) => {
  res.sendFile(path.join(__dirname, 'dist', 'index.html'));
});

// Start the server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

module.exports = app; // For testing