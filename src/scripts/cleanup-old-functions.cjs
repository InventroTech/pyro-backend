const fs = require('fs');
const path = require('path');

const functionsDir = path.join(__dirname, '../supabase/functions');

// List of functions that have been moved to the new structure
const movedFunctions = [
  'create_user_schema',
  'delete-user', 
  'invite_user',
  'send-password-reset',
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
  'recommended-lead-of-RM',
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
  'ticket-webhook',
  'assign-head',
  'task-of-RM',
  'log-failed-call-attempt',
  'feature-flag-manager',
  'link-user-email',
  '_shared',
  'test'
];

function cleanupOldFunctions() {
  console.log('🧹 Cleaning up old top-level function folders...');
  
  let removedCount = 0;
  
  movedFunctions.forEach(funcName => {
    const oldPath = path.join(functionsDir, funcName);
    
    if (fs.existsSync(oldPath)) {
      try {
        fs.rmSync(oldPath, { recursive: true, force: true });
        console.log(`🗑️  Removed old folder: ${funcName}`);
        removedCount++;
      } catch (error) {
        console.error(`❌ Failed to remove ${funcName}:`, error.message);
      }
    }
  });
  
  console.log(`\n✅ Cleanup complete! Removed ${removedCount} old function folders.`);
  console.log('📁 Your functions are now organized in the new structure:');
  console.log('   - core/auth/');
  console.log('   - core/leads/');
  console.log('   - core/tickets/');
  console.log('   - core/tasks/');
  console.log('   - common/');
  console.log('   - utils/');
}

cleanupOldFunctions(); 