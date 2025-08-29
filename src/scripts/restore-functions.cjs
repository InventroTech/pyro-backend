const fs = require('fs');
const path = require('path');

const functionsDir = path.join(__dirname, '../supabase/functions');

// Functions available in Supabase project (from the list command)
const supabaseFunctions = [
  'get-completed-ticket',
  'lead-list-of-RM',
  'create-leads',
  'create_support_ticket_webhook',
  'process-dumped-tickets',
  'task-of-RM',
  'assign-head',
  'get-all-support-tickets',
  'update-lead',
  'get-WIP-tickets',
  'pending-leads',
  'invite_user',
  'lead-list-of-OE',
  'populate-ticket',
  'delete-user',
  'get-all-leads',
  'get-lead-id',
  'delete-leads',
  'get-next-ticket',
  'get-pending-ticket',
  'get-ticket-data',
  'get-ticket-status',
  'search-ticket',
  'ticket-stats',
  'ticket-webhook',
  'converted-leads',
  'recommended-lead-of-OE',
  'recommended-lead-of-RM',
  'log-failed-call-attempt',
  'feature-flag-manager',
  'link-user-email',
  'send-password-reset',
  'create_user_schema'
];

// Function organization mapping
const functionMapping = {
  'core/auth': ['create_user_schema', 'delete-user', 'invite_user', 'send-password-reset'],
  'core/leads': ['create-leads', 'delete-leads', 'get-all-leads', 'get-lead-id', 'update-lead', 'converted-leads', 'pending-leads', 'lead-list-of-OE', 'lead-list-of-RM', 'recommended-lead-of-OE', 'recommended-lead-of-RM'],
  'core/tickets': ['create_support_ticket_webhook', 'get-all-support-tickets', 'get-completed-ticket', 'get-next-ticket', 'get-pending-ticket', 'get-ticket-data', 'get-ticket-status', 'get-WIP-tickets', 'populate-ticket', 'process-dumped-tickets', 'search-ticket', 'ticket-stats', 'ticket-webhook'],
  'core/tasks': ['assign-head', 'task-of-RM', 'log-failed-call-attempt'],
  'common': ['feature-flag-manager', 'link-user-email', '_shared'],
  'utils': ['test']
};

function createFunctionTemplate(funcName) {
  return `// Follow this setup guide to integrate the Deno language server with your editor:
// https://deno.land/manual/getting_started/setup_your_environment
// This enables autocomplete, go to definition, etc.

// Setup type definitions for built-in Supabase Runtime APIs
import "jsr:@supabase/functions-js/edge-runtime.d.ts"

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

// Get env vars from Supabase Edge runtime
const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

console.log("Hello from ${funcName}!")

Deno.serve(async (req) => {
  // Get JWT from Authorization header
  const authHeader = req.headers.get("Authorization");
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return new Response(JSON.stringify({ error: "Missing or invalid auth header" }), { status: 401 });
  }
  const jwt = authHeader.replace("Bearer ", "");

  // Decode JWT to get user id
  let userId: string | undefined;
  try {
    const payload = JSON.parse(atob(jwt.split(".")[1]));
    userId = payload.sub;
  } catch (e) {
    return new Response(JSON.stringify({ error: "Invalid JWT" }), { status: 401 });
  }
  if (!userId) {
    return new Response(JSON.stringify({ error: "No user id in JWT" }), { status: 400 });
  }

  // TODO: Implement ${funcName} logic here
  
  return new Response(JSON.stringify({ 
    success: true, 
    message: "${funcName} function called successfully",
    userId: userId 
  }), {
    headers: { "Content-Type": "application/json" },
  });
})

/* To invoke locally:

  1. Run \`supabase start\` (see: https://supabase.com/docs/reference/cli/supabase-start)
  2. Make an HTTP request:

  curl -i --location --request POST 'http://127.0.0.1:54321/functions/v1/${funcName}' \\
    --header 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0' \\
    --header 'Content-Type: application/json' \\
    --data '{"name":"Functions"}'

*/`;
}

function createDenoJson() {
  return {
    "imports": {
      "@supabase/supabase-js": "https://esm.sh/@supabase/supabase-js@2"
    }
  };
}

function createNpmrc() {
  return `@supabase:registry=https://registry.npmjs.org/
//registry.npmjs.org/:_authToken=${process.env.NPM_TOKEN || ''}`;
}

function restoreMissingFunctions() {
  console.log('🔄 Restoring missing function files...');
  
  let restoredCount = 0;
  
  Object.entries(functionMapping).forEach(([category, functions]) => {
    functions.forEach(funcName => {
      const funcPath = path.join(functionsDir, category, funcName);
      
      // Check if function folder exists but is empty or has no index.ts
      if (fs.existsSync(funcPath)) {
        const indexPath = path.join(funcPath, 'index.ts');
        if (!fs.existsSync(indexPath)) {
          // Create index.ts
          fs.writeFileSync(indexPath, createFunctionTemplate(funcName));
          console.log(`📝 Created index.ts for ${category}/${funcName}`);
          restoredCount++;
        }
        
        // Create deno.json if it doesn't exist
        const denoJsonPath = path.join(funcPath, 'deno.json');
        if (!fs.existsSync(denoJsonPath)) {
          fs.writeFileSync(denoJsonPath, JSON.stringify(createDenoJson(), null, 2));
          console.log(`📝 Created deno.json for ${category}/${funcName}`);
        }
        
        // Create .npmrc if it doesn't exist
        const npmrcPath = path.join(funcPath, '.npmrc');
        if (!fs.existsSync(npmrcPath)) {
          fs.writeFileSync(npmrcPath, createNpmrc());
          console.log(`📝 Created .npmrc for ${category}/${funcName}`);
        }
      } else {
        // Create entire function folder
        fs.mkdirSync(funcPath, { recursive: true });
        fs.writeFileSync(path.join(funcPath, 'index.ts'), createFunctionTemplate(funcName));
        fs.writeFileSync(path.join(funcPath, 'deno.json'), JSON.stringify(createDenoJson(), null, 2));
        fs.writeFileSync(path.join(funcPath, '.npmrc'), createNpmrc());
        console.log(`📁 Created complete function: ${category}/${funcName}`);
        restoredCount++;
      }
    });
  });
  
  console.log(`\n✅ Restored ${restoredCount} function files!`);
  console.log('📝 Note: These are template files. You may need to update them with the actual logic from your Supabase project.');
}

restoreMissingFunctions(); 