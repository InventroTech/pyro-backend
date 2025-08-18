const fs = require('fs');
const path = require('path');

const functionsDir = path.join(__dirname, '../supabase/functions');

// Define the organization structure
const organization = {
  'core/auth': [
    'create_user_schema',
    'delete-user', 
    'invite_user',
    'send-password-reset'
  ],
  'core/leads': [
    'create-leads',
    'delete-leads',
    'get-all-leads',
    'get-lead-id',
    'update-lead',
    'converted-leads',
    'pending-leads',
    'lead-list-of-OE',
    'lead-list-of-RM',
    'recommended-lead-of-OE',
    'recommended-lead-of-RM'
  ],
  'core/tickets': [
    'create_support_ticket_webhook',
    'get-all-support-tickets',
    'get-completed-ticket',
    'get-next-ticket',
    'get-pending-ticket',
    'get-ticket-data',
    'get-ticket-status',
    'get-WIP-tickets',
    'populate-ticket',
    'process-dumped-tickets',
    'search-ticket',
    'ticket-stats',
    'ticket-webhook'
  ],
  'core/tasks': [
    'assign-head',
    'task-of-RM',
    'log-failed-call-attempt'
  ],
  'common': [
    'feature-flag-manager',
    'link-user-email',
    '_shared'
  ],
  'utils': [
    'test'
  ]
};

function copyDirectory(src, dest) {
  if (!fs.existsSync(dest)) {
    fs.mkdirSync(dest, { recursive: true });
  }
  
  const entries = fs.readdirSync(src, { withFileTypes: true });
  
  for (const entry of entries) {
    const srcPath = path.join(src, entry.name);
    const destPath = path.join(dest, entry.name);
    
    if (entry.isDirectory()) {
      copyDirectory(srcPath, destPath);
    } else {
      fs.copyFileSync(srcPath, destPath);
    }
  }
}

function organizeFunctions() {
  console.log('🔄 Organizing Supabase Edge Functions...');
  
  // Create organized structure
  Object.entries(organization).forEach(([category, functions]) => {
    const categoryPath = path.join(functionsDir, category);
    fs.mkdirSync(categoryPath, { recursive: true });
    
    functions.forEach(funcName => {
      const sourcePath = path.join(functionsDir, funcName);
      const destPath = path.join(categoryPath, funcName);
      
      if (fs.existsSync(sourcePath)) {
        copyDirectory(sourcePath, destPath);
        console.log(`📁 Moved ${funcName} to ${category}/`);
      } else {
        console.warn(`⚠️  Function ${funcName} not found in source directory`);
      }
    });
  });
  
  console.log('✅ Function organization complete!');
}

organizeFunctions(); 