const https = require("https");
const sqlite3 = require("sqlite3").verbose();

// Defining url from which json data should be fetched.
const url =
  "https://raw.githubusercontent.com/younginnovations/internship-challenges/master/programming/petroleum-report/data.json";

const fetchDataFromUrl = () => {
  return new Promise((resolve, reject) => {
    https
      .get(url, res => {
        let data = "";
        res.on("data", chunk => {
          data += chunk;
        });
        res.on("end", () => {
          const productsData = JSON.parse(data);
          productsData.forEach(async productDetails => {
            await insertProductData(productDetails.petroleum_product);
            console.log("2");
            const productId = await getProductIdByName(
              productDetails.petroleum_product
            );
            console.log("3");
            await insertProducts(
              productDetails.year,
              productId,
              productDetails.sale
            );
            console.log("4");
          });
          resolve(true);
        });
      })
      .on("error", err => {
        reject(err.message);
      });
  });
};

const display = () => {
  return new Promise((resolve, reject) => {
    // console.log("4");
    petroleumReport.each(`SELECT * FROM report`, (error, row) => {
      if (error) {
        // return console.log(error.message);
        reject(error);
      }

      console.log(`Product` + ` ` + `Year` + ` ` + `sale`);
      console.log(row.p_id);
    });
    resolve("success");

    // console.log(row.p_id, row.year, row.sale);
  });
};

const insertProductData = product => {
  return new Promise((resolve, reject) => {
    petroleumReport.run(
      `INSERT OR IGNORE INTO products (petroleum_product) VALUES (?)`,
      [product],
      err => {
        if (err) {
          reject(err);
        }
        resolve("success");
      }
    );
  });
};

const getProductIdByName = name => {
  return new Promise((resolve, reject) => {
    petroleumReport.get(
      `SELECT p_id FROM products WHERE petroleum_product = "${name}";`,
      (err, row) => {
        if (err) {
          reject(err);
        }
        resolve(row.p_id);
      }
    );
  });
};

const insertProducts = (year, productId, sale) => {
  return new Promise((resolve, reject) => {
    petroleumReport.run(
      `INSERT INTO report (year, sale, p_id) VALUES (?, ?, ?)`,
      [year, sale, productId],
      async err => {
        if (err) {
          reject(err);
        }
        await display();
        resolve("success");
      }
    );
  });
};

// Close the database connection
const closeDatabase = () => {
  petroleumReport.close(err => {
    if (err) {
      return console.error(err.message);
    }
    console.log("Database is closed");
  });
};

// Creates a database object and store it in-memory for further operation.
let petroleumReport = new sqlite3.Database(":memory:", err => {
  // Returns error if it failed to create database.
  if (err) {
    return console.log(`Error 1: ${err.message}`);
  }
  console.log("Connected to the SQLite database.");

  petroleumReport.serialize(() => {
    petroleumReport.run(
      `CREATE TABLE IF NOT EXISTS "products" (
                "p_id" INTEGER PRIMARY KEY AUTOINCREMENT,
                "petroleum_product" TEXT NOT NULL UNIQUE)`
    );

    console.log("Products table created successfully");

    petroleumReport.run(
      `CREATE TABLE IF NOT EXISTS "report" (
                "s_id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "year"	TEXT NOT NULL,
                "sale"	INTEGER,
                "p_id"	INTEGER NOT NULL,
                CONSTRAINT "fk_product"
                    FOREIGN KEY("p_id")
                    REFERENCES "products"("p_id")
                )`
    );

    console.log("Report table created successfully");
    fetchDataFromUrl().then(() => {
      // console.log("success");
      closeDatabase();
    });
  });
});
