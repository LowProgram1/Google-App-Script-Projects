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
  'Menu': 'View.Menu.Index',
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
 * Returns list of yearly tracker tables for Menu and Tracker year selector.
 */
function getYearlyTablesList() {
  try {
    return getYearlyTablesMetadata();
  } catch (e) {
    console.error('getYearlyTablesList: ' + e.message);
    return [];
  }
}

/**
 * Fetch table metadata from tbTrackerManage for Menu UI.
 */
function fetchTableMetadata() {
  try {
    return getManageTableMetadata();
  } catch (e) {
    console.error('fetchTableMetadata: ' + e.message);
    return [];
  }
}

/**
 * Add new yearly tracker: create sheet, sync Data Validation from Template, register in tbTrackerManage.
 * @param {string} year - 4-digit year (e.g. 2026)
 * @param {string} [displayLabel] - Optional display label; defaults to year
 */
function addNewYearlyTracker(year, displayLabel) {
  try {
    var res = initializeYearlyTable(year);
    if (!res.restored) {
      addManageTableRow(res.tableName, year, (displayLabel || '').trim() || year);
      res.message = 'Year [' + year + '] generated. Sidebar updated & Data Validation synced.';
    } else {
      res.message = res.message || 'Table for ' + year + ' restored (was hidden).';
    }
    return res;
  } catch (e) {
    console.error('addNewYearlyTracker: ' + e.message);
    var msg = e.message || 'Unknown error';
    if (msg.indexOf('already exists') >= 0) throw new Error('Error: Sheet for ' + year + ' already exists or Template is missing.');
    throw new Error('Error: ' + msg);
  }
}

/**
 * Update display label for a yearly tracker in tbTrackerManage.
 */
function updateYearlyTrackerLabel(uniqueId, displayLabel) {
  try {
    updateManageTableRow(uniqueId, displayLabel);
    return { success: true };
  } catch (e) {
    console.error('updateYearlyTrackerLabel: ' + e.message);
    throw new Error(e.message);
  }
}

/**
 * Delete yearly tracker from tbTrackerManage. Optionally archive (hide) the sheet.
 */
function deleteYearlyTracker(uniqueId, archiveSheet) {
  try {
    deleteManageTableRow(uniqueId, archiveSheet !== false);
    return { success: true, message: 'Record removed.' };
  } catch (e) {
    console.error('deleteYearlyTracker: ' + e.message);
    throw new Error(e.message);
  }
}

/**
 * Sync: verify sheet exists and update status in tbTrackerManage.
 */
function syncYearlyTracker(uniqueId) {
  try {
    return syncManageTableRow(uniqueId);
  } catch (e) {
    console.error('syncYearlyTracker: ' + e.message);
    throw new Error(e.message);
  }
}

/**
 * Fetch all tracker data for a year (for post-CRUD refresh). Used to update appData after add/edit/bulk.
 */
function fetchTrackerYearData(year) {
  try {
    return getTrackerDataByProvince('', year || '');
  } catch (e) {
    console.error('fetchTrackerYearData: ' + e.message);
    return [];
  }
}

/**
 * Delete a tracker record by year and serial ID. Routes to AppTracker or AppTracker_YYYY.
 */
function deleteRecordByYear(year, id) {
  try {
    var sheetName = getTrackerSheetName(year);
    return deleteRecord(id, sheetName, 1);
  } catch (e) {
    console.error('deleteRecordByYear: ' + e.message);
    return { success: false, message: e.message };
  }
}

/**
 * One-shot batch fetch: metadata, all row data for every active year, provinces, sectors.
 * Single server call for maximum speed. Client stores in global state for instant filtering.
 */
function fetchOneShotAppData() {
  try {
    return fetchOneShotAppDataModel();
  } catch (e) {
    console.error('fetchOneShotAppData: ' + e.message);
    throw new Error(e.message);
  }
}

/**
 * One-shot fetch: return HTML for all sidebar views. Client caches and switches with no loading; tables auto-load in initViewAfterLoad.
 */
function getAllViews() {
  var out = {};
  var names = ['Dashboard', 'Menu', 'Tracker', 'Conducts', 'Category', 'Logs'];
  for (var i = 0; i < names.length; i++) {
    var viewName = names[i];
    var fileName = VIEW_MAP[viewName];
    out[viewName] = fileName ? getViewHtml(viewName, fileName) : "<div class='alert alert-danger'>View not found.</div>";
  }
  return out;
}





