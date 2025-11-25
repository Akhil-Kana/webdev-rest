import * as path from 'node:path';
import * as url from 'node:url';

import { default as express } from 'express';
import { default as sqlite3 } from 'sqlite3';

const __dirname = path.dirname(url.fileURLToPath(import.meta.url));
const db_filename = path.join(__dirname, 'db', 'stpaul_crime.sqlite3');

const port = 8000;

let app = express();
app.use(express.json());

/********************************************************************
 ***   DATABASE FUNCTIONS                                         *** 
 ********************************************************************/
// Open SQLite3 database (in read-write mode)
let db = new sqlite3.Database(db_filename, sqlite3.OPEN_READWRITE, (err) => {
    if (err) {
        console.log('Error opening ' + path.basename(db_filename));
    }
    else {
        console.log('Now connected to ' + path.basename(db_filename));
    }
});

// Create Promise for SQLite3 database SELECT query 
function dbSelect(query, params) {
    return new Promise((resolve, reject) => {
        db.all(query, params, (err, rows) => {
            if (err) {
                reject(err);
            }
            else {
                resolve(rows);
            }
        });
    });
}

// Create Promise for SQLite3 database INSERT or DELETE query
function dbRun(query, params) {
    return new Promise((resolve, reject) => {
        db.run(query, params, (err) => {
            if (err) {
                reject(err);
            }
            else {
                resolve();
            }
        });
    });
}

/********************************************************************
 ***   REST REQUEST HANDLERS                                      *** 
 ********************************************************************/

//Helper function that allows for date filtering
function isValidDateString(s) {
    // Must be YYYY-MM-DD
    if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;

    const d = new Date(s);
    return !isNaN(d.getTime());
}


// GET request handler for crime codes
app.get('/codes', (req, res) => {
    console.log(req.query); // query object (key-value pairs after the ? in the url)
    //res.status(200).type('json').send({}); // <-- you will need to change this
    let baseQuery = "SELECT code, incident_type FROM Codes";
    let params = [];

    if(req.query.code) {
        let codes = req.query.code.split(',').map(Number);
        let placeholders = codes.map(() => '?').join(',');
        baseQuery = baseQuery + ` WHERE code IN (${placeholders})`;
        params.push(...codes);
    }

    baseQuery = baseQuery + " ORDER BY code ASC";

    dbSelect(baseQuery, params)
    .then(rows => {
        let result = rows.map(r => ({
            code: r.code,
            type: r.incident_type
        }));
        res.status(200).json(result);
    })
    .catch(err => {
        console.error(err);
        res.status(500).type('txt').send("database error");
    })
});

// GET request handler for neighborhoods
app.get('/neighborhoods', (req, res) => {
    console.log(req.query); // query object (key-value pairs after the ? in the url)
    
    res.status(200).type('json').send({}); // <-- you will need to change this
});

// GET request handler for crime incidents
app.get('/incidents', async (req,res) => {
    try {
        let { start_date, end_date, code, grid, neighborhood, limit } = req.query;
        limit = limit ? Number(limit) : 1000;
        if (Number.isNaN(limit) || limit<=0) limit = 1000;

        // build where clauses
        let clauses = [];
        let params = [];

        if (start_date) {
            if (!isValidDateString(start_date)) return res.status(400).send("invalid start_date");
            clauses.push("date(date_time) >= date(?)");
            params.push(start_date);
        }
        if (end_date) {
            if (!isValidDateString(end_date)) return res.status(400).send("invalid end_date");
            clauses.push("date(date_time) <= date(?)");
            params.push(end_date);
        }
        if (code) {
            const codes = code.split(',').map(s=>Number(s.trim())).filter(n=>!Number.isNaN(n));
            if (codes.length>0) {
                let p = codes.map(()=>'?').join(',');
                clauses.push('code IN ('+p+')');
                params.push(...codes);
            }
        }
        if (grid) {
            const grids = grid.split(',').map(s=>Number(s.trim())).filter(n=>!Number.isNaN(n));
            if (grids.length>0) {
                let p = grids.map(()=>'?').join(',');
                clauses.push('police_grid IN ('+p+')');
                params.push(...grids);
            }
        }
        if (neighborhood) {
            const nbs = neighborhood.split(',').map(s=>Number(s.trim())).filter(n=>!Number.isNaN(n));
            if (nbs.length>0) {
                let p = nbs.map(()=>'?').join(',');
                clauses.push('neighborhood_number IN ('+p+')');
                params.push(...nbs);
            }
        }

        let query = 'SELECT case_number, date(date_time) as date, time(date_time) as time, code, incident, police_grid, neighborhood_number, block FROM Incidents';
        if (clauses.length>0) query += ' WHERE ' + clauses.join(' AND ');
        query += ' ORDER BY date_time DESC';
        query += ' LIMIT ?';
        params.push(limit);

        const rows = await dbSelect(query, params);
        res.json(rows);
    } catch(err) {
        console.error(err);
        res.status(500).type('txt').send("error");
    }
});

// PUT request handler for new crime incident
app.put('/new-incident', (req, res) => {
    console.log(req.body); // uploaded data
    
    res.status(200).type('txt').send('OK'); // <-- you may need to change this
});

// DELETE request handler for new crime incident
app.delete('/remove-incident', (req, res) => {
    console.log(req.body); // uploaded data
    //res.status(200).type('txt').send('OK'); // <-- you may need to change this

    let caseNumber = req.body.case_number;

    if (!caseNumber) {
        return res.status(400).type('txt').send("error: no case number")
    }

    let checkQuery = "SELECT case_number FROM Incidents WHERE case_number = ?";
    dbSelect(checkQuery, [caseNumber])
        .then(rows => {
            if (rows.length === 0) {
                return res.status(500).type('txt').send("error: case number does not exist");
            }

            let deleteQuery = "DELETE FROM Incidents WHERE case_number = ?";
            return dbRun(deleteQuery, [caseNumber])
                .then(() => {
                    res.status(200).type('txt').send("success");
                });
        })

        .catch(err => {
            console.error(err);
            res.status(500).type('txt').send("error")
        });
    

});

/********************************************************************
 ***   START SERVER                                               *** 
 ********************************************************************/
// Start server - listen for client connections
app.listen(port, () => {
    console.log('Now listening on port ' + port);
});
