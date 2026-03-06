/**
 * PNPKI DMS Model
 * Database Abstraction Layer
 *
 * Large-dataset pattern (100k+ rows): Single read (getDataRange().getValues()), process in memory
 * with JavaScript (map/filter/for), then write with one or few setValues() calls. Use batchSetValues()
 * for chunked writes (5,000 rows per call) to avoid API limits and timeouts. Never use getValue() or
 * getRange() inside loops.
 */

const DB_ID = '1QXLKOOrfHhla59-Ug4uzkaWbrpaLKjQKCL8OpK3XE7c';

/**
 * Global Connection Helper
 */
function getDbConnection() {
  try {
    return SpreadsheetApp.openById(DB_ID);
  } catch (e) {
    throw new Error("Could not connect to Spreadsheet. Please check the DB_ID.");
  }
}

/**
 * BATCH WRITE HELPER — avoids timeouts and API limits for large datasets (100k+ rows).
 * Strategy: Single setValues() per chunk (max 5,000 rows) instead of many getRange/setValue calls.
 * This bypasses Google Sheets API overhead: one read (getDataRange().getValues()), process in memory,
 * then one or few write operations. Writing in chunks of 5,000 rows avoids cell limit and execution time limits.
 * @param {SpreadsheetApp.Sheet} sheet - Target sheet
 * @param {number} startRow - 1-based row to start writing
 * @param {number} startCol - 1-based column to start writing
 * @param {any[][]} twoDimArray - 2D array [row][col]; must match destination dimensions
 */
const BATCH_WRITE_CHUNK_SIZE = 5000;
const TRACKER_TEMPLATE_SHEET = 'AppTracker_Template';
const TRACKER_METADATA_SHEET = '_TableMetadata';
const TRACKER_MANAGE_SHEET = 'tbTrackerManage';

/**
 * Resolve tracker sheet name from targetYear.
 * Legacy/temp: null/empty -> 'AppTracker_Template' (linked to template, not AppTracker which has data).
 * Yearly: 2026 -> 'AppTracker_2026'.
 */
function getTrackerSheetName(targetYear) {
  if (!targetYear || String(targetYear).trim() === '') return TRACKER_TEMPLATE_SHEET;
  return 'AppTracker_' + String(targetYear).trim();
}

/**
 * Get or create the metadata sheet for yearly table registry.
 */
function getOrCreateMetadataSheet(ss) {
  var sheet = ss.getSheetByName(TRACKER_METADATA_SHEET);
    if (!sheet) {
    sheet = ss.insertSheet(TRACKER_METADATA_SHEET);
    sheet.getRange(1, 1, 1, 3).setValues([['ID', 'TableName', 'CreatedAt']]);
    sheet.getRange(1, 1, 1, 3).setFontWeight('bold');
    sheet.hideSheet();
  }
  return sheet;
}

/**
 * Ensure tbTrackerManage sheet exists. Columns: Unique ID | Table Name | Year | Created At | Status | Display Label.
 * If new, auto-populate with existing AppTracker_2024 and AppTracker_2025.
 */
function checkAndCreateManageSheet() {
  try {
    var ss = getDbConnection();
    var sheet = ss.getSheetByName(TRACKER_MANAGE_SHEET);
    if (!sheet) {
      sheet = ss.insertSheet(TRACKER_MANAGE_SHEET);
      sheet.getRange(1, 1, 1, 6).setValues([['Unique ID', 'Table Name', 'Year', 'Created At', 'Status', 'Display Label']]);
      sheet.getRange(1, 1, 1, 6).setFontWeight('bold');
      var toAdd = [];
      var s2024 = ss.getSheetByName('AppTracker_2024');
      var s2025 = ss.getSheetByName('AppTracker_2025');
      if (s2024) toAdd.push([Utilities.getUuid().substring(0, 8), 'AppTracker_2024', '2024', new Date().toISOString(), 'Active', '2024']);
      if (s2025) toAdd.push([Utilities.getUuid().substring(0, 8), 'AppTracker_2025', '2025', new Date().toISOString(), 'Active', '2025']);
      if (toAdd.length > 0) sheet.getRange(2, 1, toAdd.length + 1, 6).setValues(toAdd);
    }
    return true;
  } catch (e) {
    console.error('checkAndCreateManageSheet: ' + e.message);
    throw new Error(e.message);
  }
}

/**
 * Fetch all rows from tbTrackerManage. Returns [{ uniqueId, tableName, year, createdAt, status, displayLabel }].
 */
function getManageTableMetadata() {
  try {
    checkAndCreateManageSheet();
    var ss = getDbConnection();
    var sheet = ss.getSheetByName(TRACKER_MANAGE_SHEET);
    if (!sheet) return [];
    var data = sheet.getDataRange().getValues();
    var out = [];
    for (var i = 1; i < data.length; i++) {
      var row = data[i];
      if (row[0]) {
        out.push({
          uniqueId: String(row[0]).trim(),
          tableName: String(row[1] || '').trim(),
          year: String(row[2] || '').trim(),
          createdAt: row[3] ? (row[3] instanceof Date ? row[3].toISOString() : String(row[3])) : '',
          status: String(row[4] || 'Active').trim(),
          displayLabel: String(row[5] || '').trim() || (row[2] ? String(row[2]) : '')
        });
      }
    }
    return out.sort(function (a, b) { return (b.year || '').localeCompare(a.year || ''); });
  } catch (e) {
    console.error('getManageTableMetadata: ' + e.message);
    return [];
  }
}

/**
 * Add a row to tbTrackerManage. Called after creating a new yearly sheet.
 */
function addManageTableRow(tableName, year, displayLabel) {
  try {
    checkAndCreateManageSheet();
    var ss = getDbConnection();
    var sheet = ss.getSheetByName(TRACKER_MANAGE_SHEET);
    if (!sheet) throw new Error('tbTrackerManage sheet not found.');
    var id = Utilities.getUuid().substring(0, 8);
    var label = (displayLabel || year || tableName || '').trim();
    sheet.appendRow([id, tableName, String(year).trim(), new Date().toISOString(), 'Active', label]);
    return { success: true, uniqueId: id };
  } catch (e) {
    console.error('addManageTableRow: ' + e.message);
    throw new Error(e.message);
  }
}

/**
 * Update display label for a row in tbTrackerManage.
 */
function updateManageTableRow(uniqueId, displayLabel) {
  try {
    var ss = getDbConnection();
    var sheet = ss.getSheetByName(TRACKER_MANAGE_SHEET);
    if (!sheet) throw new Error('tbTrackerManage sheet not found.');
    var data = sheet.getDataRange().getValues();
    for (var i = 1; i < data.length; i++) {
      if (String(data[i][0]).trim() === String(uniqueId).trim()) {
        sheet.getRange(i + 1, 6).setValue((displayLabel || '').trim());
        return { success: true };
      }
    }
    throw new Error('Record not found.');
  } catch (e) {
    console.error('updateManageTableRow: ' + e.message);
    throw new Error(e.message);
  }
}

/**
 * Delete a row from tbTrackerManage. Optionally archive (hide) the corresponding sheet.
 */
function deleteManageTableRow(uniqueId, archiveSheet) {
  try {
    var ss = getDbConnection();
    var sheet = ss.getSheetByName(TRACKER_MANAGE_SHEET);
    if (!sheet) throw new Error('tbTrackerManage sheet not found.');
    var data = sheet.getDataRange().getValues();
    var tableName = '';
    var rowIndex = -1;
    for (var i = 1; i < data.length; i++) {
      if (String(data[i][0]).trim() === String(uniqueId).trim()) {
        tableName = String(data[i][1] || '').trim();
        rowIndex = i;
        break;
      }
    }
    if (rowIndex < 0) throw new Error('Record not found.');
    if (archiveSheet && tableName) {
      var targetSheet = ss.getSheetByName(tableName);
      if (targetSheet) targetSheet.hideSheet();
    }
    var newData = data.slice(0, rowIndex).concat(data.slice(rowIndex + 1));
    sheet.clear();
    if (newData.length > 0) batchSetValues(sheet, 1, 1, newData);
    return { success: true, message: 'Record removed.' };
  } catch (e) {
    console.error('deleteManageTableRow: ' + e.message);
    throw new Error(e.message);
  }
}

/**
 * Sync: verify the sheet exists and update status if needed.
 */
function syncManageTableRow(uniqueId) {
  try {
    var ss = getDbConnection();
    var sheet = ss.getSheetByName(TRACKER_MANAGE_SHEET);
    if (!sheet) throw new Error('tbTrackerManage sheet not found.');
    var data = sheet.getDataRange().getValues();
    for (var i = 1; i < data.length; i++) {
      if (String(data[i][0]).trim() === String(uniqueId).trim()) {
        var tableName = String(data[i][1] || '').trim();
        var targetSheet = ss.getSheetByName(tableName);
        var status = targetSheet ? 'Active' : 'Archived';
        sheet.getRange(i + 1, 5).setValue(status);
        return { success: true, status: status, tableName: tableName };
      }
    }
    throw new Error('Record not found.');
  } catch (e) {
    console.error('syncManageTableRow: ' + e.message);
    throw new Error(e.message);
  }
}

/**
 * Returns list of yearly tables: { id, tableName, createdAt, year }.
 * Prefers tbTrackerManage when it exists (Status=Active). Falls back to _TableMetadata + sheet scan.
 */
function getYearlyTablesMetadata() {
  try {
    var ss = getDbConnection();
    var manageSheet = ss.getSheetByName(TRACKER_MANAGE_SHEET);
    if (manageSheet) {
      var data = manageSheet.getDataRange().getValues();
      var out = [];
      if (ss.getSheetByName(TRACKER_TEMPLATE_SHEET)) {
        out.push({ id: 'legacy', tableName: TRACKER_TEMPLATE_SHEET, createdAt: '', year: '' });
      }
      for (var i = 1; i < data.length; i++) {
        var row = data[i];
        if (row[0] && String(row[4] || 'Active').trim() === 'Active') {
          out.push({
            id: String(row[0]).trim(),
            tableName: String(row[1] || '').trim(),
            createdAt: row[3] ? (row[3] instanceof Date ? row[3].toISOString() : String(row[3])) : '',
            year: String(row[2] || '').trim()
          });
        }
      }
      if (out.length > 0) return out.sort(function (a, b) {
        if (!a.year && b.year) return 1;
        if (a.year && !b.year) return -1;
        return (b.year || '').localeCompare(a.year || '');
      });
    }
    var seen = {};
    var out = [];
    if (ss.getSheetByName(TRACKER_TEMPLATE_SHEET)) {
      out.push({ id: 'legacy', tableName: TRACKER_TEMPLATE_SHEET, createdAt: '', year: '' });
      seen[TRACKER_TEMPLATE_SHEET] = true;
    }
    var metaSheet = ss.getSheetByName(TRACKER_METADATA_SHEET);
    if (metaSheet) {
      var data = metaSheet.getDataRange().getValues();
      for (var i = 1; i < data.length; i++) {
        var row = data[i];
        if (row[0] && row[1] && !seen[row[1]]) {
          seen[row[1]] = true;
          var name = String(row[1]).trim();
          var year = name.indexOf('AppTracker_') === 0 ? name.replace('AppTracker_', '') : '';
          out.push({
            id: String(row[0]).trim(),
            tableName: name,
            createdAt: row[2] ? (row[2] instanceof Date ? row[2].toISOString() : String(row[2])) : '',
            year: year
          });
        }
      }
    }
    var sheets = ss.getSheets();
    for (var j = 0; j < sheets.length; j++) {
      var n = sheets[j].getName();
      if (n.indexOf('AppTracker_') === 0 && !seen[n]) {
        seen[n] = true;
        out.push({ id: n, tableName: n, createdAt: '', year: n.replace('AppTracker_', '') });
      }
    }
    return out.sort(function (a, b) { return (b.tableName || '').localeCompare(a.tableName || ''); });
  } catch (e) {
    console.error('getYearlyTablesMetadata: ' + e.message);
    return [];
  }
}

/**
 * Copy data validation rules from template to target sheet.
 * Uses setDataValidations(getDataValidations()) for explicit sync of dropdowns and date constraints.
 */
function copyDataValidationFromTemplate(template, targetSheet) {
  try {
    var lastRow = Math.max(2, template.getLastRow());
    var numCols = 18;
    var srcRange = template.getRange(1, 1, lastRow, numCols);
    var tgtRange = targetSheet.getRange(1, 1, lastRow, numCols);
    var validations = srcRange.getDataValidations();
    tgtRange.setDataValidations(validations);
  } catch (e) {
    console.warn('copyDataValidationFromTemplate: ' + e.message);
  }
}

/**
 * Archive (hide) the legacy AppTracker sheet if it exists and does not follow AppTracker_YYYY.
 */
function archiveLegacyTrackerSheet() {
  try {
    var ss = getDbConnection();
    var legacy = ss.getSheetByName('AppTracker');
    if (legacy) {
      legacy.hideSheet();
      return true;
    }
    return false;
  } catch (e) {
    console.warn('archiveLegacyTrackerSheet: ' + e.message);
    return false;
  }
}

/**
 * Create a new yearly tracker sheet from template. Copies data validation. Registers in metadata.
 */
function initializeYearlyTable(year) {
  try {
    var yearStr = String(year || '').trim();
    if (!yearStr || yearStr.length !== 4) throw new Error('Invalid year. Use 4 digits (e.g. 2026).');
    var yearNum = parseInt(yearStr, 10);
    if (isNaN(yearNum) || yearNum < 2020 || yearNum > 2100) throw new Error('Year must be between 2020 and 2100.');
    var tableName = 'AppTracker_' + yearStr;
    var ss = getDbConnection();
    var existing = ss.getSheetByName(tableName);
    if (existing) {
      var isHidden = false;
      try { isHidden = typeof existing.isSheetHidden === 'function' && existing.isSheetHidden(); } catch (_) {}
      if (isHidden) {
        existing.showSheet();
        checkAndCreateManageSheet();
        var manageSheet = ss.getSheetByName(TRACKER_MANAGE_SHEET);
        if (manageSheet) {
          var data = manageSheet.getDataRange().getValues();
          var found = false;
          for (var r = 1; r < data.length; r++) {
            if (String(data[r][1] || '').trim() === tableName) {
              manageSheet.getRange(r + 1, 5).setValue('Active');
              found = true;
              break;
            }
          }
          if (!found) addManageTableRow(tableName, yearStr, yearStr);
        }
        return { success: true, tableName: tableName, restored: true, message: 'Table for ' + yearStr + ' restored (was hidden).' };
      }
      throw new Error('Sheet for ' + yearStr + ' already exists.');
    }
    var template = ss.getSheetByName(TRACKER_TEMPLATE_SHEET);
    if (!template) {
      var legacy = ss.getSheetByName('AppTracker');
      if (!legacy) throw new Error('Template is missing. Create AppTracker_Template or AppTracker sheet first.');
      var headers = legacy.getRange(1, 1, 1, 18).getValues();
      template = ss.insertSheet(TRACKER_TEMPLATE_SHEET);
      template.getRange(1, 1, 1, 18).setValues(headers);
      template.getRange(1, 1, 1, 18).setFontWeight('bold');
      template.hideSheet();
    }
    var newSheet = template.copyTo(ss);
    newSheet.setName(tableName);
    newSheet.showSheet();
    copyDataValidationFromTemplate(template, newSheet);
    var metaSheet = getOrCreateMetadataSheet(ss);
    var id = Utilities.getUuid().substring(0, 8);
    var createdAt = new Date().toISOString();
    metaSheet.appendRow([id, tableName, createdAt]);
    return { success: true, tableName: tableName, id: id, createdAt: createdAt };
  } catch (e) {
    console.error('initializeYearlyTable: ' + e.message);
    throw new Error(e.message);
  }
}

/**
 * Delete an application record by year and serial (unique ID).
 */
function deleteApplicationRecord(year, uniqueId) {
  try {
    var sheetName = getTrackerSheetName(year);
    var res = deleteRecord(uniqueId, sheetName, 1);
    if (res.success) res.message = 'Record successfully removed from ' + (year || 'Legacy') + ' Tracker.';
    return res;
  } catch (e) {
    console.error('deleteApplicationRecord: ' + e.message);
    return { success: false, message: e.message };
  }
}

/**
 * One-shot fetch: metadata from tbTrackerManage + row data for each active year.
 * Returns { metadata: [...], years: { '2026': [...], '2025': [...], 'legacy': [...] }, provinces: [...] }.
 * Uses defaultProv for backward compatibility; pass empty string to getTrackerDataByProvince for "All Provinces".
 */
function fetchAllYearlyData() {
  try {
    var ss = getDbConnection();
    checkAndCreateManageSheet();
    archiveLegacyTrackerSheet();
    var rawMeta = getManageTableMetadata().filter(function (m) { return String(m.status || 'Active').trim() === 'Active'; });
    var metadata = rawMeta.map(function(m) { return { id: m.uniqueId, tableName: m.tableName, year: m.year, displayLabel: m.displayLabel, uniqueId: m.uniqueId }; });
    var provinces = getProvincesForTrackerTabs();
    var years = {};
    for (var i = 0; i < metadata.length; i++) {
      var m = metadata[i];
      var year = String(m.year || '').trim();
      var key = year || 'legacy';
      try {
        var data = getTrackerDataByProvince('', year);
        years[key] = data || [];
      } catch (err) {
        years[key] = [];
      }
    }
    if (ss.getSheetByName(TRACKER_TEMPLATE_SHEET)) {
      var hasLegacy = metadata.some(function(x) { return x.tableName === TRACKER_TEMPLATE_SHEET; });
      if (!hasLegacy) {
        metadata.unshift({ id: 'legacy', year: '', tableName: TRACKER_TEMPLATE_SHEET, displayLabel: 'temp', uniqueId: 'legacy' });
      }
      if (!years['legacy']) {
        try {
          years['legacy'] = getTrackerDataByProvince('', '');
        } catch (e) {
          years['legacy'] = [];
        }
      }
    }
    return { metadata: metadata, years: years, provinces: provinces };
  } catch (e) {
    console.error('fetchAllYearlyData: ' + e.message);
    throw new Error(e.message);
  }
}

/**
 * One-shot batch fetch: metadata, all row data for every active year, provinces, sectors.
 * Single server call for maximum speed. Client uses this for instant filtering and rendering.
 */
function fetchOneShotAppDataModel() {
  try {
    var sectors = getSectorList();
    var payload = fetchAllYearlyData();
    payload.sectors = sectors || [];
    return payload;
  } catch (e) {
    console.error('fetchOneShotAppDataModel: ' + e.message);
    throw new Error(e.message);
  }
}

function batchSetValues(sheet, startRow, startCol, twoDimArray) {
  if (!twoDimArray || twoDimArray.length === 0) return;
  var numCols = twoDimArray[0].length;
  var totalRows = twoDimArray.length;
  if (totalRows <= BATCH_WRITE_CHUNK_SIZE) {
    sheet.getRange(startRow, startCol, totalRows, numCols).setValues(twoDimArray);
    return;
  }
  for (var offset = 0; offset < totalRows; offset += BATCH_WRITE_CHUNK_SIZE) {
    var chunk = twoDimArray.slice(offset, offset + BATCH_WRITE_CHUNK_SIZE);
    var chunkRows = chunk.length;
    sheet.getRange(startRow + offset, startCol, chunkRows, numCols).setValues(chunk);
  }
}

/**
 * Repository: Fetch and map tracker data. Optional targetYear routes to AppTracker_YYYY.
 */
function getTrackerData(targetYear) {
  try {
    const ss = getDbConnection();
    const sheetName = getTrackerSheetName(targetYear);
    const sheet = ss.getSheetByName(sheetName);
    
    if (!sheet) throw new Error("Sheet '" + sheetName + "' not found.");
    
    const values = sheet.getDataRange().getValues();
    
    // If sheet is empty or only has headers
    if (values.length <= 1) return []; 
    
    // Remove header row
    const headers = values.shift();
    
    // SORTING LOGIC: Sort by Column A (Timestamp) Descending
    values.sort((a, b) => {
      const dateA = new Date(a[0]);
      const dateB = new Date(b[0]);
      return dateB - dateA; // Newest first
    });

    // MAP DATA: 18 columns. P=15=File URL, Q=16=Type of Application, R=17=Certificate Status (legacy 16 cols = File in P/15)
    const hasNewCols = (r) => r && r.length >= 18;
    const normalizeType = (val) => {
      const t = String(val || "").trim();
      return (t === "Bulk" || t.toLowerCase() === "bulk") ? "Bulk" : "Individual";
    };
    const normalizeCert = (val) => {
      const t = String(val || "").trim();
      return (t === "Revoked" || t.toLowerCase() === "revoked") ? "Revoked" : "Active";
    };
    return values.map((row, index) => ({
      rowNumber: index + 1,
      serial: row[1] || "",
      lastName: row[2] || "",
      firstName: row[3] || "",
      middleName: row[4] || "",
      suffix: row[5] || "",
      email: row[6] || "",
      contact: row[7] || "",
      gender: row[8] || "",
      agency: row[9] || "",
      sector: row[10] || "",
      province: row[11] || "",
      city: row[12] || "",
      district: row[13] || "",
      interviewDate: row[14] instanceof Date ? row[14].toLocaleDateString() : (row[14] || ""),
      typeOfApplication: hasNewCols(row) ? normalizeType(row[16]) : "Individual",
      certificateStatus: hasNewCols(row) ? normalizeCert(row[17]) : "Active",
      fileUrl: hasNewCols(row) ? (row[15] || "") : (row[15] || "")
    }));
    
  } catch (e) {
    console.error(e.toString());
    throw new Error("Database Error: " + e.message);
  }
}

/**
 * Returns list of { domainCode, provinceName } for building province tabs in App Tracker.
 * Single read from Cities sheet; unique by domain + province name.
 */
function getProvincesForTrackerTabs() {
  try {
    var ss = getDbConnection();
    var sheet = getCitiesSheet(ss);
    var data = sheet.getDataRange().getValues();
    if (data.length <= 1) return [];
    var hasDomain = _citiesRowHasDomain(data, 1);
    var seen = {};
    var out = [];
    for (var i = 1; i < data.length; i++) {
      var domain = _domainAt(data[i], hasDomain);
      var name = _provinceAt(data[i], hasDomain);
      if (!name) continue;
      var key = (domain || '') + '|' + String(name).trim();
      if (!seen[key]) {
        seen[key] = true;
        out.push({ domainCode: (domain || '').trim(), provinceName: String(name).trim() });
      }
    }
    return out;
  } catch (e) {
    console.error('getProvincesForTrackerTabs: ' + e.message);
    return [];
  }
}

/**
 * Tracker data filtered by province name (column L). Optional targetYear routes to AppTracker_YYYY.
 */
function getTrackerDataByProvince(provinceName, targetYear) {
  try {
    var ss = getDbConnection();
    var sheetName = getTrackerSheetName(targetYear);
    var sheet = ss.getSheetByName(sheetName);
    if (!sheet) throw new Error("Sheet '" + sheetName + "' not found.");
    var values = sheet.getDataRange().getValues();
    if (values.length <= 1) return [];
    var headers = values.shift();
    var provinceCol = 11; // Column L
    var match = (provinceName && String(provinceName).trim()) ? String(provinceName).trim().toLowerCase() : null;
    if (match) {
      values = values.filter(function (row) {
        return String(row[provinceCol] || '').trim().toLowerCase() === match;
      });
    }
    values.sort(function (a, b) {
      var dateA = new Date(a[0]);
      var dateB = new Date(b[0]);
      return dateB - dateA;
    });
    var hasNewCols = function (r) { return r && r.length >= 18; };
    var normalizeType = function (val) {
      var t = String(val || '').trim();
      return (t === 'Bulk' || t.toLowerCase() === 'bulk') ? 'Bulk' : 'Individual';
    };
    var normalizeCert = function (val) {
      var t = String(val || '').trim();
      return (t === 'Revoked' || t.toLowerCase() === 'revoked') ? 'Revoked' : 'Active';
    };
    return values.map(function (row, index) {
      return {
        rowNumber: index + 1,
        serial: row[1] || '',
        lastName: row[2] || '',
        firstName: row[3] || '',
        middleName: row[4] || '',
        suffix: row[5] || '',
        email: row[6] || '',
        contact: row[7] || '',
        gender: row[8] || '',
        agency: row[9] || '',
        sector: row[10] || '',
        province: row[11] || '',
        city: row[12] || '',
        district: row[13] || '',
        interviewDate: row[14] instanceof Date ? row[14].toLocaleDateString() : (row[14] || ''),
        typeOfApplication: hasNewCols(row) ? normalizeType(row[16]) : 'Individual',
        certificateStatus: hasNewCols(row) ? normalizeCert(row[17]) : 'Active',
        fileUrl: hasNewCols(row) ? (row[15] || '') : (row[15] || '')
      };
    });
  } catch (e) {
    console.error(e.toString());
    throw new Error('Database Error: ' + e.message);
  }
}

function getNextUserNumber(domain, provCode, targetYear) {
  try {
    const ss = getDbConnection();
    const sheetName = getTrackerSheetName(targetYear);
    const sheet = ss.getSheetByName(sheetName);
    const data = sheet.getDataRange().getValues();
    
    let startAt = 1;
    if (domain === "015") {
      const points015 = { "A": 7000, "B": 8000, "C": 14000, "D": 17500 };
      startAt = points015[provCode] || 1;
    } else if (domain === "017") {
      const points017 = { "A": 6000, "B": 6500, "C": 7000 };
      startAt = points017[provCode] || 1;
    }

    if (data.length <= 1) return startAt;

    let maxNum = 0;
    const prefix = domain + provCode; 

    for (let i = 1; i < data.length; i++) {
      let serial = String(data[i][1]).replace(/-/g, ""); 
      
      if (serial.indexOf(prefix) === 0) {
        // We skip the prefix (length 4) AND the sector character (length 1)
        // Example: 015BN007001 -> prefix is 015B (4). Sector is N (1). 
        // Number starts at index 5.
        let numPart = parseInt(serial.substring(prefix.length + 1)); 
        if (!isNaN(numPart) && numPart > maxNum) {
          maxNum = numPart;
        }
      }
    }

    return (maxNum === 0) ? startAt : maxNum + 1;
  } catch (e) {
    return 1;
  }
}

function toProperCase(str) {
  if (!str) return "";
  return str.toString().trim().toLowerCase().split(' ').map(word => {
    return word.charAt(0).toUpperCase() + word.slice(1);
  }).join(' ');
}

/**
 * CRUD all data to Spreadsheet, including the hidden Timestamp
 */
function saveApplication(formData, fileData, targetYear) {
  try {
    const ss = getDbConnection();
    const sheetName = getTrackerSheetName(targetYear);
    const sheet = ss.getSheetByName(sheetName);
    if (!sheet) throw new Error("Sheet '" + sheetName + "' not found.");

    // --- 1. DATA SANITIZATION ---
    formData.email = formData.email ? formData.email.trim().toLowerCase() : "";
    let cleanContact = (formData.contact || "").replace(/[\s\-\(\)]/g, "");
    
    const isValidContact = /^09\d{9}$/.test(cleanContact) || /^639\d{9}$/.test(cleanContact) || /^\d{7,10}$/.test(cleanContact);
    if (!isValidContact) return { success: false, message: "Invalid contact number format." };
    formData.contact = cleanContact;

    // --- 1b. LOCKED PROVINCE VALIDATION (when user is on a province tab) ---
    if (formData.lockedDomain && formData.lockedProvince) {
      var reqDomain = String(formData.lockedDomain).trim();
      var reqProvince = String(formData.lockedProvince).trim();
      var submittedDomain = String(formData.domain || '').trim();
      var submittedProvince = (formData.provinceName && formData.provinceName.trim())
        ? formData.provinceName.trim()
        : (getProvinceNameByCode(formData.province, formData.domain) || '').trim();
      if (submittedDomain !== reqDomain || submittedProvince !== reqProvince) {
        return { success: false, message: "Domain or Province cannot be changed for this tab." };
      }
    }

    // --- 2. FIND ROW INDEX (For Updates) ---
    let rowIndex = -1;
    const data = sheet.getDataRange().getValues();
    
    if (formData.originalSerial) {
      for (let i = 1; i < data.length; i++) {
        if (data[i][1] == formData.originalSerial) {
          rowIndex = i + 1;
          break;
        }
      }
      if (rowIndex === -1) throw new Error("Original record not found.");
    }

    // --- 3. HANDLE FILE PROCESSING ---
    const fileUrlCol = rowIndex !== -1 && data[rowIndex - 1].length >= 18 ? 15 : 15;
    let finalFileUrl = rowIndex !== -1 ? (data[rowIndex - 1][fileUrlCol] || "") : "";

    if (fileData && fileData.base64) { 
      // A. Trash the old file if it exists
      if (finalFileUrl && finalFileUrl.includes('id=')) {
        try {
          const oldFileId = finalFileUrl.split('id=')[1];
          DriveApp.getFileById(oldFileId).setTrashed(true);
        } catch (err) {
          console.warn("Could not trash old file: " + err.message);
        }
      } else if (finalFileUrl && finalFileUrl.includes('/d/')) {
        // Handle URL format: /d/FILE_ID/view
        try {
          const oldFileId = finalFileUrl.split('/d/')[1].split('/')[0];
          DriveApp.getFileById(oldFileId).setTrashed(true);
        } catch (err) {
          console.warn("Could not trash old file: " + err.message);
        }
      }

      // B. Upload the new file
      const folder = DriveApp.getFolderById('1GfI9TVVQg3aBcnrlDVD3Y1A0h_5GJEoz');
      const decodedData = Utilities.base64Decode(fileData.base64);

      const fName = toProperCase(formData.firstName || "");
      const lName = toProperCase(formData.lastName || "");
      const mName = formData.middleName ? toProperCase(formData.middleName) : "";
      const sfx   = formData.suffix ? formData.suffix.toUpperCase() : "";
      
      const nameParts = [lName, fName, mName, sfx].filter(p => p.trim() !== "");
      const blob = Utilities.newBlob(decodedData, fileData.type, nameParts.join("_"));
      const file = folder.createFile(blob);
      file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
      finalFileUrl = file.getUrl();
    }

    // --- 4. PREPARE ROW DATA ---
    let typeOfApp = formData.typeOfApplication || "Individual";
    let certStatus = formData.certificateStatus || "Active";
    if (rowIndex !== -1) {
      const existingRow = data[rowIndex - 1];
      if (existingRow.length >= 18) {
        if (!formData.typeOfApplication) typeOfApp = existingRow[16] || "Individual";
        if (!formData.certificateStatus) certStatus = existingRow[17] || "Active";
      }
    }
    const rowValues = [
      new Date(),                      // A: Timestamp
      formData.serial,                 // B: Serial
      toProperCase(formData.lastName), // C: Last Name
      toProperCase(formData.firstName),// D: First Name
      (formData.middleName && formData.middleName !== "N/A") ? toProperCase(formData.middleName) : "", // E: Middle Name
      (formData.suffix && formData.suffix !== "N/A") ? formData.suffix.trim() : "", // F: Suffix
      formData.email,                  // G
      formData.contact,                // H
      formData.gender,                 // I
      formData.agency,                 // J
      getSectorAliasByCode(formData.sector), // K
      (formData.provinceName && formData.provinceName.trim()) ? formData.provinceName.trim() : (getProvinceNameByCode(formData.province, formData.domain) || formData.province), // L
      formData.city,                   // M
      formData.district,               // N
      formData.interviewDate,          // O: Interview Date
      finalFileUrl,                    // P: File URL
      typeOfApp,                       // Q: Type of Application (Individual/Bulk)
      certStatus                       // R: Certificate Status (Active/Revoked)
    ];

    // --- 5. COMMIT TO SHEET ---
    if (rowIndex !== -1) {
      sheet.getRange(rowIndex, 1, rowIndex, rowValues.length).setValues([rowValues]);
      return { success: true, message: "Application updated successfully!" };
    } else {
      sheet.appendRow(rowValues);
      return { success: true, message: "Application saved successfully!" };
    }

  } catch (e) {
    console.error("Server Error: " + e.toString());
    return { success: false, message: e.toString() };
  }
}

/**
 * GETS A SINGLE RECORD BY SERIAL NUMBER. Optional targetYear routes to AppTracker_YYYY.
 */
function getRecordBySerial(serial, targetYear) {
  try {
    const ss = getDbConnection();
    const sheetName = getTrackerSheetName(targetYear);
    const sheet = ss.getSheetByName(sheetName);
    if (!sheet) throw new Error("Sheet '" + sheetName + "' not found.");

    const data = sheet.getDataRange().getValues();
    
    const hasNewCols = (r) => r && r.length >= 18;
    for (let i = 1; i < data.length; i++) {
      if (data[i][1] == serial) { 
        const row = data[i];
        const fileUrlCol = hasNewCols(row) ? 15 : 15;
        return {
          serial: row[1],
          lastName: row[2],
          firstName: row[3],
          middleName: row[4],
          suffix: row[5],
          email: row[6],
          contact: row[7],
          gender: row[8],
          agency: row[9],
          sector: row[10],
          province: row[11],
          city: row[12],
          district: row[13],
          interviewDate: row[14] instanceof Date ?
            Utilities.formatDate(row[14], Session.getScriptTimeZone(), "yyyy-MM-dd") : (row[14] || ""),
          typeOfApplication: hasNewCols(row) ? (row[16] || "Individual") : "Individual",
          certificateStatus: hasNewCols(row) ? (row[17] || "Active") : "Active",
          fileUrl: row[fileUrlCol] || ""
        };
      }
    }
    return null; 
  } catch (e) {
    console.error("Backend Error: " + e.message);
    throw new Error(e.message);
  }
}

/**
 * One-call load for Edit form: record + province list + city list for that record's domain/province.
 * Reduces 3 round-trips to 1 so the edit modal appears faster.
 */
function getRecordBySerialForEdit(serial, targetYear) {
  var record = getRecordBySerial(serial, targetYear);
  if (!record) return null;
  var domain = String(record.serial || "").substring(0, 3);
  var provList = getUniqueProvincesFromCitiesByDomain(domain);
  var provCode = "";
  for (var i = 0; i < provList.length; i++) {
    if (String(provList[i].name || "").trim().toUpperCase() === String(record.province || "").trim().toUpperCase()) {
      provCode = provList[i].code;
      break;
    }
  }
  var cityList = provCode ? getCitiesForProvince(provCode, domain, record.province) : [];
  return { record: record, provList: provList || [], cityList: cityList || [] };
}

/**
 * Single read → remove row in memory → rewrite sheet with batchSetValues (chunked if large).
 * Avoids deleteRow() which can be slow on huge sheets; one read and one chunked write scales to 100k+ rows.
 */
function deleteRecord(id, sheetName, colIndex) {
  try {
    const ss = getDbConnection();
    const sheet = ss.getSheetByName(sheetName);
    if (!sheet) throw new Error("Sheet not found: " + sheetName);

    const data = sheet.getDataRange().getValues();
    var rowIndex = -1;

    for (var i = 1; i < data.length; i++) {
      if (data[i][colIndex] == id) {
        rowIndex = i;
        break;
      }
    }

    if (rowIndex !== -1) {
      const delRow = data[rowIndex];
      const fileUrlCol = 15;
      const fileUrl = delRow[fileUrlCol];
      if (fileUrl && (fileUrl.includes('id=') || fileUrl.includes('/d/'))) {
        try {
          var fileId = "";
          if (fileUrl.includes('id=')) {
            fileId = fileUrl.split('id=')[1].split('&')[0];
          } else {
            fileId = fileUrl.split('/d/')[1].split('/')[0];
          }
          DriveApp.getFileById(fileId).setTrashed(true);
        } catch (err) {
          console.warn("File could not be trashed (it may not exist): " + err.message);
        }
      }

      // Remove row in memory; rewrite sheet in one or chunked setValues to avoid API overhead.
      var newData = data.slice(0, rowIndex).concat(data.slice(rowIndex + 1));
      sheet.clear();
      batchSetValues(sheet, 1, 1, newData);
      return { success: true, message: "Record and associated file removed successfully." };
    } else {
      return { success: false, message: "Record not found." };
    }
  } catch (e) {
    return { success: false, message: e.toString() };
  }
}

function processBulkUpload(dataRows, targetYear) {
  const ss = getDbConnection();
  const sheetName = getTrackerSheetName(targetYear);
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) throw new Error("Sheet '" + sheetName + "' not found.");
  const lastRow = sheet.getLastRow();
  const sessionCounters = {};
  const uploadTimestamp = new Date();
  const uploadDateStr = Utilities.formatDate(uploadTimestamp, Session.getScriptTimeZone(), "yyyy-MM-dd");

  const rowsToAdd = dataRows.map(row => {
    const domain = String(row.domain || "").trim();
    const provCode = String(row.provCode || "").trim();
    const sectorCode = String(row.sectorCode || "").trim();

    const counterKey = domain + provCode;
    let nextNum;
    if (sessionCounters[counterKey]) {
      sessionCounters[counterKey]++;
      nextNum = sessionCounters[counterKey];
    } else {
      nextNum = getNextUserNumber(domain, provCode, targetYear);
      sessionCounters[counterKey] = nextNum;
    }

    const paddedNum = String(nextNum).padStart(6, '0');
    const generatedSerial = `${domain}${provCode}${sectorCode}${paddedNum}`;

    return [
      uploadTimestamp,      // Col 1: Timestamp
      generatedSerial,      // Col 2: Serial
      row.lastName,         // Col 3
      row.firstName,        // Col 4
      row.middleName,       // Col 5
      row.suffix,           // Col 6
      row.email,            // Col 7
      row.contact,          // Col 8
      row.gender,           // Col 9
      row.agency,           // Col 10
      row.sectorName,       // Col 11
      row.provinceName,     // Col 12
      row.cityName,         // Col 13
      row.district || "",   // Col 14: District
      uploadDateStr,        // Col 15: Interview Date = bulk upload date
      "",                   // Col 16 (P): File URL
      "Bulk",               // Col 17 (Q): Type of Application
      "Active"              // Col 18 (R): Certificate Status
    ];
  });

  if (rowsToAdd.length > 0) {
    // Write in chunks of 5,000 rows to stay under API limits and avoid timeouts; single setValues per chunk bypasses per-row API overhead.
    batchSetValues(sheet, lastRow + 1, 1, rowsToAdd);
    return { success: true, count: rowsToAdd.length };
  }
}

// --- Sector sheet: A=Timestamp, B=UniqueID, C=Sector Name, D=Sector Alias, E=Code (no No. column; row count is display-only) ---
const SECTOR_SHEET_NAME = 'Sector';
const SECTOR_COL_CODE = 4; // E = 0-based index for Code (used for delete/update lookup)

function getSectorSheet(ss) {
  let sheet = ss.getSheetByName(SECTOR_SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(SECTOR_SHEET_NAME);
    sheet.getRange(1, 1, 1, 5).setValues([['Timestamp', 'UniqueID', 'Sector Name', 'Sector Alias', 'Code']]);
    sheet.getRange(1, 1, 1, 5).setFontWeight('bold');
  }
  return sheet;
}

/**
 * Returns list of sectors for Tracker dropdown. Code is used in serial; alias is stored in AppTracker and shown in table.
 */
function getSectorList() {
  try {
    const ss = getDbConnection();
    const sheet = getSectorSheet(ss);
    const data = sheet.getDataRange().getValues();
    if (data.length <= 1) return [];
    const rows = data.slice(1);
    return rows
      .filter(function (row) { return row[4]; }) // Code (E) present
      .map(function (row) {
        return {
          code: String(row[4]).trim(),
          alias: String(row[3] || '').trim(),
          name: String(row[2] || '').trim()
        };
      });
  } catch (e) {
    console.error('getSectorList: ' + e.message);
    return [];
  }
}

/**
 * Resolve sector code to alias (for saving to AppTracker). Returns alias or original if not found.
 */
function getSectorAliasByCode(code) {
  const list = getSectorList();
  const found = list.find(function (s) { return s.code === String(code).trim(); });
  return found ? found.alias : (code || '');
}

/**
 * Data for Category view Sector table. No. is row count only (not stored in DB).
 */
function getSectorData() {
  try {
    const ss = getDbConnection();
    const sheet = getSectorSheet(ss);
    const data = sheet.getDataRange().getValues();
    if (data.length <= 1) return [];
    const rows = data.slice(1);
    return rows.map(function (row, idx) {
      return {
        rowNumber: idx + 1,
        code: String(row[4] || '').trim(),
        sectorName: String(row[2] || '').trim(),
        alias: String(row[3] || '').trim()
      };
    });
  } catch (e) {
    console.error('getSectorData: ' + e.message);
    return [];
  }
}

/**
 * Create a new sector. Code must be a single character for serial generation. No. is not stored; row count is display-only.
 */
function createSector(sectorName, sectorAlias, code) {
  try {
    const ss = getDbConnection();
    const sheet = getSectorSheet(ss);
    const data = sheet.getDataRange().getValues();
    const nextUniqueId = String(data.length).padStart(3, '0');
    const codeStr = String(code || '').trim().charAt(0);
    if (!codeStr) throw new Error('Sector Code is required (one character for serial number).');
    const row = [
      new Date(),
      nextUniqueId,
      String(sectorName || '').trim(),
      String(sectorAlias || '').trim(),
      codeStr
    ];
    sheet.appendRow(row);
    SpreadsheetApp.flush();
    return { success: true, message: 'Sector created successfully.' };
  } catch (e) {
    console.error('createSector: ' + e.message);
    return { success: false, message: e.message };
  }
}

/**
 * Get one sector by code (for edit form).
 */
function getSectorByCode(code) {
  try {
    const ss = getDbConnection();
    const sheet = getSectorSheet(ss);
    const data = sheet.getDataRange().getValues();
    for (var i = 1; i < data.length; i++) {
      if (String(data[i][SECTOR_COL_CODE]).trim() === String(code).trim()) {
        return {
          code: String(data[i][4]).trim(),
          sectorName: String(data[i][2] || '').trim(),
          alias: String(data[i][3] || '').trim()
        };
      }
    }
    return null;
  } catch (e) {
    console.error('getSectorByCode: ' + e.message);
    return null;
  }
}

/**
 * Update an existing sector by code.
 * Single read → update row in memory → one setValues() to avoid per-cell API overhead.
 */
function updateSector(oldCode, sectorName, sectorAlias, newCode) {
  try {
    const ss = getDbConnection();
    const sheet = getSectorSheet(ss);
    const data = sheet.getDataRange().getValues();
    for (var i = 1; i < data.length; i++) {
      if (String(data[i][SECTOR_COL_CODE]).trim() === String(oldCode).trim()) {
        data[i][2] = String(sectorName || '').trim();
        data[i][3] = String(sectorAlias || '').trim();
        data[i][4] = String(newCode || '').trim().charAt(0);
        sheet.getRange(i + 1, 1, i + 1, data[i].length).setValues([data[i]]);
        return { success: true, message: 'Sector updated successfully.' };
      }
    }
    return { success: false, message: 'Sector not found.' };
  } catch (e) {
    console.error('updateSector: ' + e.message);
    return { success: false, message: e.message };
  }
}

// --- Cities sheet: A=Timestamp, B=UniqueID, C=Domain Code, D=Province Code, E=Province, F=City/Municipality, G=District (7 cols) ---
const CITIES_SHEET_NAME = 'Cities';

function getCitiesSheet(ss) {
  var sheet = ss.getSheetByName(CITIES_SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(CITIES_SHEET_NAME);
    sheet.getRange(1, 1, 1, 7).setValues([['Timestamp', 'UniqueID', 'Domain Code', 'Province Code', 'Province', 'City/Municipality', 'District']]);
    sheet.getRange(1, 1, 1, 7).setFontWeight('bold');
  }
  return sheet;
}

function _citiesRowHasDomain(data, i) {
  return data[0] && data[0].length >= 7;
}

function _domainAt(row, hasDomain) { return hasDomain ? String(row[2] || '').trim() : ''; }
function _provCodeAt(row, hasDomain) { return hasDomain ? String(row[3] || '').trim() : String(row[2] || '').trim(); }
function _provinceAt(row, hasDomain) { return hasDomain ? String(row[4] || '').trim() : String(row[3] || '').trim(); }
function _cityAt(row, hasDomain) { return hasDomain ? String(row[5] || '').trim() : String(row[4] || '').trim(); }
function _districtAt(row, hasDomain) { return hasDomain ? String(row[6] || '').trim() : String(row[5] || '').trim(); }

/**
 * Unique provinces from Cities table (all domains). For Category Add City when no domain filter.
 */
function getUniqueProvincesFromCities() {
  try {
    var ss = getDbConnection();
    var sheet = getCitiesSheet(ss);
    var data = sheet.getDataRange().getValues();
    if (data.length <= 1) return [];
    var hasDomain = _citiesRowHasDomain(data, 1);
    var seen = {};
    var out = [];
    for (var i = 1; i < data.length; i++) {
      var code = _provCodeAt(data[i], hasDomain);
      var name = _provinceAt(data[i], hasDomain);
      if (!code) continue;
      var key = hasDomain ? _domainAt(data[i], hasDomain) + '|' + code : code;
      if (!seen[key]) {
        seen[key] = true;
        out.push({ code: code, name: name || code });
      }
    }
    return out;
  } catch (e) {
    console.error('getUniqueProvincesFromCities: ' + e.message);
    return [];
  }
}

/**
 * Unique provinces for a given domain. Uses (domain, code, name) so domain 017 can show
 * both Maguindanao del Sur and Maguindanao del Norte even when both use province code "A".
 */
function getUniqueProvincesFromCitiesByDomain(domainCode) {
  try {
    var ss = getDbConnection();
    var sheet = getCitiesSheet(ss);
    var data = sheet.getDataRange().getValues();
    if (data.length <= 1) return [];
    var hasDomain = _citiesRowHasDomain(data, 1);
    var domain = String(domainCode || '').trim();
    var seen = {};
    var out = [];
    for (var i = 1; i < data.length; i++) {
      var rowDomain = _domainAt(data[i], hasDomain);
      if (hasDomain && rowDomain !== domain) continue;
      var code = _provCodeAt(data[i], hasDomain);
      var name = _provinceAt(data[i], hasDomain);
      if (!code) continue;
      var key = domain + '|' + code + '|' + (name || code);
      if (!seen[key]) {
        seen[key] = true;
        out.push({ code: code, name: name || code });
      }
    }
    return out;
  } catch (e) {
    console.error('getUniqueProvincesFromCitiesByDomain: ' + e.message);
    return [];
  }
}

/**
 * Cities (and district) for a province. Optional domain filter. When provinceName is provided
 * (e.g. for domain 017 where code "A" has both Maguindanao del Sur and Maguindanao del Norte),
 * filters by province name so only cities for that province are returned.
 */
function getCitiesForProvince(provinceCode, domainCode, provinceName) {
  try {
    var ss = getDbConnection();
    var sheet = getCitiesSheet(ss);
    var data = sheet.getDataRange().getValues();
    if (data.length <= 1) return [];
    var hasDomain = _citiesRowHasDomain(data, 1);
    var code = String(provinceCode || '').trim();
    var domain = domainCode ? String(domainCode).trim() : '';
    var provName = provinceName ? String(provinceName).trim() : '';
    var out = [];
    for (var i = 1; i < data.length; i++) {
      if (_provCodeAt(data[i], hasDomain) !== code) continue;
      if (hasDomain && domain && _domainAt(data[i], hasDomain) !== domain) continue;
      if (provName && String(_provinceAt(data[i], hasDomain)).trim() !== provName) continue;
      out.push({
        city: _cityAt(data[i], hasDomain),
        district: _districtAt(data[i], hasDomain)
      });
    }
    return out;
  } catch (e) {
    console.error('getCitiesForProvince: ' + e.message);
    return [];
  }
}

function getProvinceNameByCode(provinceCode, domainCode) {
  var list = domainCode ? getUniqueProvincesFromCitiesByDomain(domainCode) : getUniqueProvincesFromCities();
  var found = list.find(function (p) { return p.code === String(provinceCode).trim(); });
  return found ? found.name : (provinceCode || '');
}

/**
 * Data for Category Cities table. No. is row count only. Includes domainCode.
 */
function getCitiesData() {
  try {
    var ss = getDbConnection();
    var sheet = getCitiesSheet(ss);
    var data = sheet.getDataRange().getValues();
    if (data.length <= 1) return [];
    var hasDomain = _citiesRowHasDomain(data, 1);
    var rows = data.slice(1);
    return rows.map(function (row, idx) {
      return {
        rowNumber: idx + 1,
        domainCode: _domainAt(row, hasDomain),
        provinceCode: _provCodeAt(row, hasDomain),
        province: _provinceAt(row, hasDomain),
        city: _cityAt(row, hasDomain),
        district: _districtAt(row, hasDomain)
      };
    });
  } catch (e) {
    console.error('getCitiesData: ' + e.message);
    return [];
  }
}

function createCity(domainCode, provinceCode, province, city, district) {
  try {
    var ss = getDbConnection();
    var sheet = getCitiesSheet(ss);
    var data = sheet.getDataRange().getValues();
    var lastCol = data[0] ? data[0].length : 0;
    var nextUniqueId = String(data.length).padStart(3, '0');
    var row = [
      new Date(),
      nextUniqueId,
      String(domainCode || '').trim(),
      String(provinceCode || '').trim(),
      String(province || '').trim(),
      String(city || '').trim(),
      String(district || '').trim()
    ];
    if (lastCol < 7) {
      for (var c = lastCol; c < 7; c++) row.push('');
      row = row.slice(0, 7);
    }
    sheet.appendRow(row);
    return { success: true, message: 'City created successfully.' };
  } catch (e) {
    console.error('createCity: ' + e.message);
    return { success: false, message: e.message };
  }
}

function getCityByCodeAndCity(domainCode, provinceCode, cityName) {
  try {
    var ss = getDbConnection();
    var sheet = getCitiesSheet(ss);
    var data = sheet.getDataRange().getValues();
    var hasDomain = _citiesRowHasDomain(data, 1);
    var domain = String(domainCode || '').trim();
    var code = String(provinceCode || '').trim();
    var city = String(cityName || '').trim();
    for (var i = 1; i < data.length; i++) {
      if (_provCodeAt(data[i], hasDomain) !== code || _cityAt(data[i], hasDomain) !== city) continue;
      if (hasDomain && domain && _domainAt(data[i], hasDomain) !== domain) continue;
      return {
        domainCode: _domainAt(data[i], hasDomain),
        provinceCode: _provCodeAt(data[i], hasDomain),
        province: _provinceAt(data[i], hasDomain),
        city: _cityAt(data[i], hasDomain),
        district: _districtAt(data[i], hasDomain)
      };
    }
    return null;
  } catch (e) {
    console.error('getCityByCodeAndCity: ' + e.message);
    return null;
  }
}

/**
 * Single read → update row in memory → one setValues() to avoid per-cell API overhead.
 */
function updateCity(oldDomain, oldProvinceCode, oldCityName, domainCode, provinceCode, province, city, district) {
  try {
    var ss = getDbConnection();
    var sheet = getCitiesSheet(ss);
    var data = sheet.getDataRange().getValues();
    var hasDomain = _citiesRowHasDomain(data, 1);
    var oDomain = String(oldDomain || '').trim();
    var oCode = String(oldProvinceCode || '').trim();
    var oCity = String(oldCityName || '').trim();
    for (var i = 1; i < data.length; i++) {
      if (_provCodeAt(data[i], hasDomain) !== oCode || _cityAt(data[i], hasDomain) !== oCity) continue;
      if (hasDomain && oDomain && _domainAt(data[i], hasDomain) !== oDomain) continue;
      data[i][2] = String(domainCode || '').trim();
      data[i][3] = String(provinceCode || '').trim();
      data[i][4] = String(province || '').trim();
      data[i][5] = String(city || '').trim();
      data[i][6] = String(district || '').trim();
      sheet.getRange(i + 1, 1, i + 1, data[i].length).setValues([data[i]]);
      return { success: true, message: 'City updated successfully.' };
    }
    return { success: false, message: 'City not found.' };
  } catch (e) {
    console.error('updateCity: ' + e.message);
    return { success: false, message: e.message };
  }
}

/**
 * Single read → remove row in memory → rewrite sheet with batchSetValues (chunked if large).
 * Avoids deleteRow() for scalability with large datasets.
 */
function deleteCity(domainCode, provinceCode, cityName) {
  try {
    var ss = getDbConnection();
    var sheet = getCitiesSheet(ss);
    var data = sheet.getDataRange().getValues();
    var hasDomain = _citiesRowHasDomain(data, 1);
    var domain = String(domainCode || '').trim();
    var code = String(provinceCode || '').trim();
    var city = String(cityName || '').trim();
    for (var i = 1; i < data.length; i++) {
      if (_provCodeAt(data[i], hasDomain) !== code || _cityAt(data[i], hasDomain) !== city) continue;
      if (hasDomain && domain && _domainAt(data[i], hasDomain) !== domain) continue;
      var newData = data.slice(0, i).concat(data.slice(i + 1));
      sheet.clear();
      batchSetValues(sheet, 1, 1, newData);
      return { success: true, message: 'City removed successfully.' };
    }
    return { success: false, message: 'City not found.' };
  } catch (e) {
    console.error('deleteCity: ' + e.message);
    return { success: false, message: e.message };
  }
}

// --- Agency sheet: A=Timestamp (hidden in UI), B=UniqueID, C=Agency Name, D=Agency Alias, E=Sector, F=Agency Head, G=Position, H=Agency Email, I=Contact Person, J=Contact Number, K=Contact Email ---
const AGENCY_SHEET_NAME = 'Agency';

function getAgencySheet(ss) {
  var sheet = ss.getSheetByName(AGENCY_SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(AGENCY_SHEET_NAME);
    sheet.getRange(1, 1, 1, 11).setValues([['Timestamp', 'UniqueID', 'Agency Name', 'Agency Alias', 'Sector', 'Agency Head', 'Position', 'Agency Email', 'Contact Person', 'Contact Number', 'Contact Email']]);
    sheet.getRange(1, 1, 1, 11).setFontWeight('bold');
  }
  return sheet;
}

function getAgencyData() {
  try {
    var ss = getDbConnection();
    var sheet = getAgencySheet(ss);
    var data = sheet.getDataRange().getValues();
    if (data.length <= 1) return [];
    var rows = data.slice(1);
    return rows.map(function (row, idx) {
      return {
        rowNumber: idx + 1,
        uniqueId: String(row[1] || '').trim(),
        agencyName: String(row[2] || '').trim(),
        agencyAlias: String(row[3] || '').trim(),
        sector: String(row[4] || '').trim(),
        agencyHead: String(row[5] || '').trim(),
        position: String(row[6] || '').trim(),
        agencyEmail: String(row[7] || '').trim(),
        contactPerson: String(row[8] || '').trim(),
        contactNumber: String(row[9] || '').trim(),
        contactEmail: String(row[10] || '').trim()
      };
    });
  } catch (e) {
    console.error('getAgencyData: ' + e.message);
    return [];
  }
}

function createAgency(agencyName, agencyAlias, sector, agencyHead, position, agencyEmail, contactPerson, contactNumber, contactEmail) {
  try {
    var ss = getDbConnection();
    var sheet = getAgencySheet(ss);
    var data = sheet.getDataRange().getValues();
    var nextUniqueId = String(data.length).padStart(3, '0');
    var row = [
      new Date(),
      nextUniqueId,
      String(agencyName || '').trim(),
      String(agencyAlias || '').trim(),
      String(sector || '').trim(),
      String(agencyHead || '').trim(),
      String(position || '').trim(),
      String(agencyEmail || '').trim(),
      String(contactPerson || '').trim(),
      String(contactNumber || '').trim(),
      String(contactEmail || '').trim()
    ];
    sheet.appendRow(row);
    return { success: true, message: 'Agency created successfully.' };
  } catch (e) {
    console.error('createAgency: ' + e.message);
    return { success: false, message: e.message };
  }
}

function getAgencyByUniqueId(uniqueId) {
  try {
    var ss = getDbConnection();
    var sheet = getAgencySheet(ss);
    var data = sheet.getDataRange().getValues();
    for (var i = 1; i < data.length; i++) {
      if (String(data[i][1]).trim() === String(uniqueId).trim()) {
        var row = data[i];
        return {
          uniqueId: String(row[1] || '').trim(),
          agencyName: String(row[2] || '').trim(),
          agencyAlias: String(row[3] || '').trim(),
          sector: String(row[4] || '').trim(),
          agencyHead: String(row[5] || '').trim(),
          position: String(row[6] || '').trim(),
          agencyEmail: String(row[7] || '').trim(),
          contactPerson: String(row[8] || '').trim(),
          contactNumber: String(row[9] || '').trim(),
          contactEmail: String(row[10] || '').trim()
        };
      }
    }
    return null;
  } catch (e) {
    console.error('getAgencyByUniqueId: ' + e.message);
    return null;
  }
}

/**
 * Single read → update row in memory → one setValues() to avoid per-cell API overhead.
 */
function updateAgency(uniqueId, agencyName, agencyAlias, sector, agencyHead, position, agencyEmail, contactPerson, contactNumber, contactEmail) {
  try {
    var ss = getDbConnection();
    var sheet = getAgencySheet(ss);
    var data = sheet.getDataRange().getValues();
    for (var i = 1; i < data.length; i++) {
      if (String(data[i][1]).trim() === String(uniqueId).trim()) {
        data[i][0] = new Date();
        data[i][2] = String(agencyName || '').trim();
        data[i][3] = String(agencyAlias || '').trim();
        data[i][4] = String(sector || '').trim();
        data[i][5] = String(agencyHead || '').trim();
        data[i][6] = String(position || '').trim();
        data[i][7] = String(agencyEmail || '').trim();
        data[i][8] = String(contactPerson || '').trim();
        data[i][9] = String(contactNumber || '').trim();
        data[i][10] = String(contactEmail || '').trim();
        sheet.getRange(i + 1, 1, i + 1, data[i].length).setValues([data[i]]);
        return { success: true, message: 'Agency updated successfully.' };
      }
    }
    return { success: false, message: 'Agency not found.' };
  } catch (e) {
    console.error('updateAgency: ' + e.message);
    return { success: false, message: e.message };
  }
}

function deleteAgency(uniqueId) {
  try {
    return deleteRecord(uniqueId, AGENCY_SHEET_NAME, 1);
  } catch (e) {
    console.error('deleteAgency: ' + e.message);
    return { success: false, message: e.message };
  }
}

/**
 * Returns agencies from Agency table for a given sector (alias e.g. "NGA", or code e.g. "N").
 * Matches Agency sheet Sector column (column E); accepts alias or code and matches stored alias or code.
 * Used by Tracker to populate Agency dropdown when sector is selected.
 */
function getAgenciesBySector(sectorCodeOrAlias) {
  try {
    var ss = getDbConnection();
    var sheet = getAgencySheet(ss);
    var data = sheet.getDataRange().getValues();
    if (data.length <= 1) return [];
    var raw = String(sectorCodeOrAlias || '').trim();
    if (!raw) return [];
    var aliasFromCode = getSectorAliasByCode(raw);
    var matchAlias = (aliasFromCode && aliasFromCode !== raw) ? aliasFromCode.trim().toLowerCase() : raw.toLowerCase();
    var matchCode = raw.toLowerCase();
    var out = [];
    for (var i = 1; i < data.length; i++) {
      var rowSector = String(data[i][4] || '').trim().toLowerCase();
      if (rowSector !== matchAlias && rowSector !== matchCode) continue;
      var name = String(data[i][2] || '').trim();
      if (name) out.push({ name: name, alias: String(data[i][3] || '').trim() });
    }
    return out;
  } catch (e) {
    console.error('getAgenciesBySector: ' + e.message);
    return [];
  }
}


