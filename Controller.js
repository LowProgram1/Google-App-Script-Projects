/**
 * PNPKI DMS Controller
 * Handles routing and server-side logic
 */

function doGet(e) {
  return HtmlService.createTemplateFromFile('Index')
      .evaluate()
      .setTitle('PNPKI - DMS')
      .addMetaTag('viewport', 'width=device-width, initial-scale=1')
      .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.DEFAULT); // Safer for internal App Scripts
}

/**
 * Inclusion helper for modular UI components
 */
function include(filename) {
  return HtmlService.createTemplateFromFile(filename).getRawContent();
}

/**
 * Service Layer: Get User Identity
 * Ensures only @gov.ph domains are authorized
 */
function getAuthenticatedUser() {
  try {
    // getEffectiveUser is more stable in Web Apps than getActiveUser
    const userEmail = Session.getEffectiveUser().getEmail(); 
    
    if (!userEmail) return "Guest User";
    
    // If you MUST restrict to gov.ph, do it here
    if (userEmail.indexOf('.gov.ph') === -1) {
       return "Unauthorized (" + userEmail + ")";
    }
    
    return userEmail;
  } catch (e) {
    return "User Identity Hidden";
  }
}
var VIEW_MAP = {
  'Dashboard': 'Dashboard',
  'Tracker': 'View.Tracker.Index',
  'Conducts': 'View.Conducts.Index',
  'Category': 'View.Category.Index',
  'Logs': 'Logs'
};

function getViewHtml(viewName, fileName) {
  try {
    var template = HtmlService.createTemplateFromFile(fileName);
    return template.evaluate().getContent();
  } catch (e) {
    console.error("View Load Error: " + e.message);
    return "<div class='alert alert-danger'><strong>Error loading " + viewName + ":</strong><br>" + (e.message || "") + "</div>";
  }
}

/**
 * Controller: Handles dynamic view loading with strict naming
 */
function getView(viewName) {
  var fileName = VIEW_MAP[viewName];
  if (!fileName) return "<div class='alert alert-danger'>View logic not found.</div>";
  return getViewHtml(viewName, fileName);
}

/**
 * One-shot fetch: return HTML for all sidebar views. Client caches and switches with no loading; tables auto-load in initViewAfterLoad.
 */
function getAllViews() {
  var out = {};
  var names = ['Dashboard', 'Tracker', 'Conducts', 'Category', 'Logs'];
  for (var i = 0; i < names.length; i++) {
    var viewName = names[i];
    var fileName = VIEW_MAP[viewName];
    out[viewName] = fileName ? getViewHtml(viewName, fileName) : "<div class='alert alert-danger'>View not found.</div>";
  }
  return out;
}





