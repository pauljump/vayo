/**
 * Inspect what API calls the Stuytown website actually makes
 */
const puppeteer = require('puppeteer');

async function inspectAPI() {
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  const page = await browser.newPage();
  const apiCalls = [];

  // Intercept all network requests
  await page.on('request', request => {
    const url = request.url();
    if (url.includes('units.stuytown.com') || url.includes('/api/')) {
      apiCalls.push({
        url,
        method: request.method(),
        headers: request.headers(),
        postData: request.postData()
      });
    }
  });

  try {
    console.log('Loading Stuytown page...');
    await page.goto('https://www.stuytown.com/nyc-apartments-for-rent/', {
      waitUntil: 'networkidle2',
      timeout: 60000
    });

    console.log('Waiting for page to load...');
    await page.waitForTimeout(5000);

    console.log('\n=== API Calls Made ===');
    apiCalls.forEach((call, i) => {
      console.log(`\n[${i + 1}] ${call.method} ${call.url}`);
      if (call.postData) {
        console.log(`  Body: ${call.postData}`);
      }
    });

    // Try to count visible units
    const unitCount = await page.evaluate(() => {
      const cards = document.querySelectorAll('[class*="card"]');
      return cards.length;
    });

    console.log(`\n=== Visible Unit Cards: ${unitCount} ===`);

  } catch (error) {
    console.error('Error:', error);
  } finally {
    await browser.close();
  }
}

inspectAPI();
