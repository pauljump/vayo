//
// scrape_stuytown_csv.js
//
// Now uses Stuytown's JSON API and captures ALL available data
// Outputs both JSON (complete data) and CSV (basic fields for compatibility)
//
const fs = require('fs');
const https = require('https');

/**
 * Fetch JSON data from Stuytown API
 */
function fetchAPI(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(e);
        }
      });
    }).on('error', reject);
  });
}

// 2) The main function that fetches from Stuytown API and captures EVERYTHING
async function scrapeStuytownAndWriteCSV() {
  console.log('Fetching apartment data from all properties...');

  // Fetch ALL properties (Stuytown, PCV, Kips Bay Court, Parker Towers, 8 Spruce)
  // itemsOnPage=500 ensures we get everything in one request
  const API_URL = 'https://units.stuytown.com/api/units?itemsOnPage=500&Order=low-price';

  const stats = {
    totalFetched: 0,
    validListings: 0,
    invalidListings: 0,
  };

  try {
    const response = await fetchAPI(API_URL);
    const units = response.unitModels || [];
    stats.totalFetched = units.length;

    console.log(`Fetched ${units.length} units from API`);

    const today = new Date().toISOString().slice(0, 10);
    const csvRows = [];
    const completeData = {
      scrapedAt: new Date().toISOString(),
      count: units.length,
      units: []
    };

    for (const unit of units) {
      // Build complete enriched unit object with ALL fields
      const enrichedUnit = {
        // Core identifiers
        unitSpk: unit.unitSpk,
        unitNumber: unit.unitNumber,

        // Basic specs
        bedrooms: unit.bedrooms,
        bathrooms: unit.bathrooms,
        sqft: unit.sqft,

        // Pricing
        price: unit.price,
        unitRates: unit.unitRates, // Multi-term pricing
        isSurchargeIncluded: unit.isSurchargeIncluded,
        isCapped: unit.isCapped,

        // Availability
        availableDate: unit.availableDate,
        isAvailable: unit.isAvailable,

        // Features
        isFlex: unit.isFlex,
        hasExtraBedroom: unit.hasExtraBedroom,
        finish: unit.finish,

        // Incentives
        incentives: unit.incentives || [],
        uniqueIncentives: unit.uniqueIncentives,
        hideIncentives: unit.hideIncentives,

        // Media
        virtualTourUrl: unit.virtualTourUrl,
        images: unit.images || [],
        floorplan: unit.floorplan,

        // Amenities (full granular list)
        amenities: unit.amenities || [],

        // Property info
        property: unit.property,

        // Building (complete with coords, transit)
        building: unit.building,

        // Metadata
        description: unit.description,

        // Computed helper fields for easy querying
        _computed: {
          fullAddress: formatAddress(unit),
          bedBathFormatted: formatBedBath(unit.bedrooms, unit.bathrooms),
          availableDateFormatted: formatAvailableDate(unit.availableDate),
          priceFormatted: `$${unit.price.toLocaleString()}`,
          hasWasherDryer: hasWasherDryer(unit.amenities),
          propertyName: unit.property?.name || 'Stuytown',
          floorNumber: extractFloorNumber(unit.amenities),
          pricePerSqft: unit.sqft ? Math.round(unit.price / unit.sqft) : null,
          finishLevel: unit.finish?.name || 'Unknown',
          hasVirtualTour: !!unit.virtualTourUrl,
          imageCount: (unit.images || []).length,
          transitOptions: (unit.building?.transports || []).map(t => t.description).join(', ')
        }
      };

      completeData.units.push(enrichedUnit);

      // Also build CSV row for backwards compatibility
      const csvListing = {
        unit: enrichedUnit._computed.fullAddress,
        bb: enrichedUnit._computed.bedBathFormatted,
        available: enrichedUnit._computed.availableDateFormatted,
        price: enrichedUnit._computed.priceFormatted,
        washer: enrichedUnit._computed.hasWasherDryer,
        flex: unit.isFlex,
        property: enrichedUnit._computed.propertyName
      };

      if (!isValidListing(csvListing)) {
        stats.invalidListings++;
        console.warn(`  Invalid listing skipped: ${csvListing.unit || 'unknown'}`);
        continue;
      }

      stats.validListings++;

      csvRows.push([
        today,
        csvListing.unit,
        csvListing.bb,
        csvListing.available,
        csvListing.price,
        csvListing.washer ? 'TRUE' : 'FALSE',
        csvListing.flex ? 'TRUE' : 'FALSE',
        csvListing.property,
      ]);
    }

    // Write complete JSON data
    console.log('\nWriting complete data to out.json ...');
    fs.writeFileSync('out.json', JSON.stringify(completeData, null, 2), 'utf8');

    // Write CSV for compatibility
    console.log('Writing summary to out.csv ...');
    writeCSV('out.csv', csvRows);

    console.log(`\n=== Fetch Complete ===`);
    console.log(`Total fetched: ${stats.totalFetched}`);
    console.log(`Valid listings: ${stats.validListings}`);
    console.log(`Invalid (skipped): ${stats.invalidListings}`);
    console.log(`Written to:`);
    console.log(`  - out.json (complete data, ${completeData.units.length} units)`);
    console.log(`  - out.csv (summary, ${csvRows.length} rows)`);

  } catch (error) {
    console.error(`\n❌ Error fetching from API:`, error.message);
    throw error;
  }
}

/**
 * Format building address and unit number
 */
function formatAddress(unit) {
  const building = unit.building || {};
  const address = building.address || '';
  const unitNum = unit.unitNumber || '';

  if (address && unitNum) {
    return `${address}, Unit ${unitNum}`;
  } else if (address) {
    return address;
  } else if (unitNum) {
    return `Unit ${unitNum}`;
  }
  return 'Unknown';
}

/**
 * Format bed/bath as "X Bed X Bath" or "Studio"
 */
function formatBedBath(bedrooms, bathrooms) {
  if (bedrooms === 0) {
    return `Studio ${bathrooms} Bath`;
  }
  return `${bedrooms} Bed ${bathrooms} Bath`;
}

/**
 * Format available date from ISO timestamp
 */
function formatAvailableDate(isoDate) {
  if (!isoDate) return '';
  try {
    const date = new Date(isoDate);
    return date.toISOString().slice(0, 10); // YYYY-MM-DD
  } catch (e) {
    return isoDate;
  }
}

/**
 * Check if unit has washer/dryer amenity
 */
function hasWasherDryer(amenities) {
  if (!amenities || !Array.isArray(amenities)) return false;
  return amenities.some(a =>
    a.code === 'WasherDryer' ||
    a.code === 'Washer/Dryer' ||
    (a.description && /washer.*dryer/i.test(a.description))
  );
}

/**
 * Extract floor number from amenities
 */
function extractFloorNumber(amenities) {
  if (!amenities || !Array.isArray(amenities)) return null;

  for (const amenity of amenities) {
    // Look for floor amenity codes like "FS02" = 2nd floor
    if (amenity.code && /^FS\d+$/.test(amenity.code)) {
      return parseInt(amenity.code.replace('FS', ''), 10);
    }
    // Also check description
    if (amenity.friendlyDescription && /(\d+)(st|nd|rd|th)\s+floor/i.test(amenity.friendlyDescription)) {
      const match = amenity.friendlyDescription.match(/(\d+)(st|nd|rd|th)\s+floor/i);
      return parseInt(match[1], 10);
    }
  }
  return null;
}

/**
 * Validates listing data from API
 * @param {Object} listing - Listing object with unit, bb, price, available
 * @returns {boolean} - True if valid, false otherwise
 */
function isValidListing(listing) {
  const { unit, bb, price, available } = listing;

  // Unit must exist and be non-empty
  if (!unit || unit.trim() === '') {
    return false;
  }

  // Bed/bath should match expected patterns
  if (bb && !/(?:Studio|\d+)\s*(?:Bed|Bath)/i.test(bb)) {
    // Allow missing bb, but if present, validate format
    return false;
  }

  // Price must exist and match currency format
  if (!price || !/^\$[\d,]+$/.test(price)) {
    return false;
  }

  // Convert price to number and validate range (reasonable NYC apartment prices)
  const priceNum = parseInt(price.replace(/[\$,]/g, ''), 10);
  if (isNaN(priceNum) || priceNum < 500 || priceNum > 50000) {
    return false;
  }

  // Available date is optional but should contain date-like content if present
  if (available && available.trim() !== '' && !/\d/.test(available)) {
    // Available should have some date/number if it's set
    return false;
  }

  return true;
}

/**
 * writeCSV: writes rows to CSV with headers:
 *   Date,Unit,BB,Available,Price,Washer,Flex,Property
 */
function writeCSV(filename, rows) {
  // We'll build the CSV lines manually
  // 1) Add a header line
  const header = 'Date,Unit,BB,Available,Price,Washer,Flex,Property';

  // 2) Convert each row (array) to a comma-separated line, quoting fields with commas
  const lines = rows.map(row => {
    // row is [today, unit, bb, available, price, washer, flex, property]
    // Quote any field that contains commas
    return row.map(cell => {
      const str = String(cell);
      if (str.includes(',') || str.includes('"') || str.includes('\n')) {
        return `"${str.replace(/"/g, '""')}"`;
      }
      return str;
    }).join(',');
  });

  // 3) Combine header + lines
  const csvString = [header, ...lines].join('\n');

  // 4) Write to file
  fs.writeFileSync(filename, csvString, 'utf8');
}

//---------------------------------------
// MAIN
//---------------------------------------
(async () => {
  await scrapeStuytownAndWriteCSV();
})();
