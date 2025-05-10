document.addEventListener("DOMContentLoaded", function () {
    const homeTabButton = document.getElementById("datachat-tab-button");
    loadContent('datachat-tab', homeTabButton);
    const overview = document.getElementById('app-overview');
    const mainContent = document.getElementById('main-content');
    const proceedBtn = document.getElementById('proceed-btn');
    const backBtn = document.getElementById('back-to-home-btn');

    if (proceedBtn) {
        proceedBtn.addEventListener('click', function () {
            overview.style.display = 'none';
            mainContent.style.display = '';
        });
    }
    if (backBtn) {
        backBtn.addEventListener('click', function () {
            mainContent.style.display = 'none';
            overview.style.display = '';
        });
    }
    proceedBtn.click();
});


let activeTabButton = null;
const sidebarButtons = document.querySelectorAll('.sidebar .btn');
const tabContent = document.getElementById('tabContent');


function loadContent(tabId, button) {
    if (activeTabButton) {
        activeTabButton.classList.remove('active');
    }
    button.classList.add('active');
    activeTabButton = button;

    const allContents = tabContent.querySelectorAll('.tab');
    allContents.forEach(content => content.classList.remove('active'));

    const contentDiv = document.getElementById(tabId);
    if (contentDiv) {
        contentDiv.classList.add('active');
    }
}

sidebarButtons.forEach(button => {
    button.addEventListener('click', function (event) {
        event.preventDefault();
        const tabId = this.getAttribute('data-tab');

        if (tabId) {
            loadContent(tabId, this);
        }
    });
});

// ------------------ Module Imports ------------------
import sqlite3InitModule from "https://esm.sh/@sqlite.org/sqlite-wasm@3.46.1-build3";
import { render, html } from "https://cdn.jsdelivr.net/npm/lit-html@3/+esm";
import { dsvFormat, autoType } from "https://cdn.jsdelivr.net/npm/d3-dsv@3/+esm";
import { Marked } from "https://cdn.jsdelivr.net/npm/marked@13/+esm";
import { markedHighlight } from "https://cdn.jsdelivr.net/npm/marked-highlight@2/+esm";
import hljs from "https://cdn.jsdelivr.net/npm/highlight.js@11/+esm";



const defaultDB = "mydb.sqlite";
const sqlite3 = await sqlite3InitModule({ printErr: console.error });

// ------------------ DOM Elements ------------------
const $upload = document.getElementById("upload");
const $tablesContainer = document.getElementById("tables-container");
const $sql = document.getElementById("sql");
const $result = document.getElementById("result");
let latestQueryResult = [];
let queryHistory = [];

// ------------------ Markdown Setup ------------------
const marked = new Marked(
    markedHighlight({
        langPrefix: "hljs language-",
        highlight(code, lang) {
            const language = hljs.getLanguage(lang) ? lang : "plaintext";
            return hljs.highlight(code, { language }).value;
        },
    })
);
marked.use({
    renderer: {
        table(header, body) {
            return `<table class="table table-sm">${header}${body}</table>`;
        },
    },
});

// ------------------ Fetch LLM Token (Optional) ------------------
let token;
try {
    token = (
        await fetch("https://llmfoundry.straivedemo.com/token", {
            credentials: "include",
        }).then((r) => r.json())
    ).token;
} catch {
    token = null;
}


render(
    token
        ? html`
        <div class="mb-3 d-none">
            <label for="file" class="btn btn-secondary btn-sm" style="margin-top:1rem">Upload CSV <i class="bi bi-upload"></i></label>
            <input
                class="form-control"
                type="file"
                id="file"
                name="file"
                accept=".csv,.sqlite3,.db,.sqlite,.s3db,.sl3"
                multiple
                style="display: none;"
            />
        </div>
        `
        : html`<a class="btn btn-primary" href="https://llmfoundry.straivedemo.com/">
          Sign in to upload files
        </a>`,
    $upload
);

const db = new sqlite3.oo1.DB(defaultDB, "c");
const DB = {
    context: "",
    schema: function () {
        let tables = [];
        db.exec("SELECT name, sql FROM sqlite_master WHERE type='table'", {
            rowMode: "object",
        }).forEach((table) => {
            table.columns = db.exec(`PRAGMA table_info(${table.name})`, {
                rowMode: "object",
            });
            tables.push(table);
        });
        return tables;
    },

    upload: async function (file) {
        const newFileName = `database${file.name.match(/\..+$/)[0]}`;

        const newFile = new File([file], newFileName, { type: file.type });

        if (newFile.name.match(/\.(sqlite3|sqlite|db|s3db|sl3)$/i)) {
            await DB.uploadSQLite(newFile);
        } else if (newFile.name.match(/\.csv$/i)) {
            await DB.uploadDSV(newFile, ",");
        } else if (newFile.name.match(/\.tsv$/i)) {
            await DB.uploadDSV(newFile, "\t");
        } else {
            notify("danger", "Unknown file type", newFile.name);
        }
    },

    uploadSQLite: async function (file) {
        const fileReader = new FileReader();
        await new Promise((resolve) => {
            fileReader.onload = async (e) => {
                await sqlite3.capi.sqlite3_js_posix_create_file(
                    file.name,
                    e.target.result
                );

                const uploadDB = new sqlite3.oo1.DB(file.name, "r");
                const tables = uploadDB.exec(
                    "SELECT name, sql FROM sqlite_master WHERE type='table'",
                    { rowMode: "object" }
                );
                for (const { name, sql } of tables) {
                    db.exec(`DROP TABLE IF EXISTS "${name}"`);
                    db.exec(sql);
                    const data = uploadDB.exec(`SELECT * FROM "${name}"`, {
                        rowMode: "object",
                    });
                    if (data.length > 0) {
                        const columns = Object.keys(data[0]);
                        const insertSQL = `INSERT INTO "${name}" (${columns
                            .map((c) => `"${c}"`)
                            .join(", ")}) VALUES (${columns.map(() => "?").join(", ")})`;
                        const stmt = db.prepare(insertSQL);
                        db.exec("BEGIN TRANSACTION");
                        for (const row of data) {
                            stmt.bind(columns.map((c) => row[c])).stepReset();
                        }
                        db.exec("COMMIT");
                        stmt.finalize();
                    }
                }
                uploadDB.close();
                resolve();
            };
            fileReader.readAsArrayBuffer(file);
        });
    },

    uploadDSV: async function (file, separator) {
        const fileReader = new FileReader();
        const result = await new Promise((resolve) => {
            fileReader.onload = (e) => {
                const rows = dsvFormat(separator).parse(e.target.result, autoType);
                resolve(rows);
            };
            fileReader.readAsText(file);
        });
        const tableName = file.name
            .slice(0, -4)
            .replace(/[^a-zA-Z0-9_]/g, "_");

        await DB.insertRows(tableName, result);

    },

    insertRows: async function (tableName, rows) {
        const cols = Object.keys(rows[0]);
        const typeMap = {};

        for (let col of cols) {
            const sampleValue = rows[0][col];
            if (typeof sampleValue === "string") {
                if (sampleValue.match(/^\d{4}-\d{2}-\d{2}$/)) {
                    typeMap[col] = "TEXT";
                } else if (sampleValue.match(/^\d{2}:\d{2}:\d{2}$/)) {
                    typeMap[col] = "TEXT";
                } else if (sampleValue.match(/^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$/)) {
                    const dateCol = `${col}_date`;
                    const timeCol = `${col}_time`;

                    typeMap[dateCol] = "TEXT";
                    typeMap[timeCol] = "TEXT";
                } else if (sampleValue.match(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/)) {
                    const dateCol = `${col}_date`;
                    const timeCol = `${col}_time`;

                    typeMap[dateCol] = "TEXT";
                    typeMap[timeCol] = "TEXT";
                } else {
                    typeMap[col] = "TEXT";
                }
            } else if (typeof sampleValue === "number") {
                typeMap[col] = Number.isInteger(sampleValue) ? "INTEGER" : "REAL";
            } else if (typeof sampleValue === "boolean") {
                typeMap[col] = "INTEGER";
            } else if (sampleValue instanceof Date) {
                typeMap[col] = "TEXT";
            }
        }

        const createSQL = `CREATE TABLE IF NOT EXISTS ${tableName} (
            ${Object.keys(typeMap).map((col) => `"${col}" ${typeMap[col]}`).join(", ")}
        )`;
        db.exec(createSQL);

        // Prepare insert statement
        let newCols = Object.keys(typeMap);
        const insertSQL = `INSERT INTO ${tableName} (${newCols.map((c) => `"${c}"`).join(", ")}) VALUES (${newCols.map(() => "?").join(", ")})`;

        const stmt = db.prepare(insertSQL);
        db.exec("BEGIN TRANSACTION");

        for (const row of rows) {
            let values = [];
            for (let col of newCols) {
                if (col.endsWith("_date") || col.endsWith("_time")) {
                    let originalCol = col.replace(/_(date|time)$/, "");
                    if (row[originalCol]) {

                        let regexDateTime = /^(?:(\d{4}-\d{2}-\d{2})|(\d{2}-\d{2}-\d{4})) (\d{2}:\d{2})(?::\d{2})?$/;
                        let matches = row[originalCol].match(regexDateTime);
                        if (matches) {
                            let datePart = matches[1] || matches[2];
                            let timePart = matches[3];

                            if (matches[2]) {
                                const [day, month, year] = datePart.split('-');
                                datePart = `${year}-${month}-${day}`;
                            }

                            if (col.endsWith("_date")) {
                                values.push(datePart);
                            } else if (col.endsWith("_time")) {
                                values.push(timePart);
                            }
                        } else {
                            console.warn(`Invalid date format for column: ${originalCol}, Value: ${row[originalCol]}`);
                            values.push(null);
                        }
                    } else {
                        values.push(null);
                    }
                } else {
                    values.push(
                        row[col] instanceof Date
                            ? row[col].toISOString().split('T')[0]
                            : row[col]
                    );
                }
            }
            stmt.bind(values).stepReset();
        }

        db.exec("COMMIT");
        stmt.finalize();

        if (typeof db !== 'undefined' && db) {
            fetchTickets();
            uploadCSV();
        } else {
            console.error("SQLite DB object 'db' not found. Cannot initialize ticket table.");
            render(html`<tr><td colspan="9" class="text-center p-5 text-danger">Database connection not available.</td></tr>`, tableBody);
        }
    }
}



// ------------------ Handle File Selection ------------------
$upload.addEventListener("change", async (e) => {
    const files = Array.from(e.target.files);
    for (let file of files) {
        await DB.upload(file);
    }
    drawTables();
});

async function llm({ system, user, schema }) {
    const response = await fetch("https://llmfoundry.straivedemo.com/openai/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}:datachat` },
        body: JSON.stringify({
            model: "gpt-4.1-mini",
            messages: [
                { role: "system", content: system },
                { role: "user", content: user },
            ],
            temperature: 0,
            ...(schema ? { response_format: { type: "json_schema", json_schema: { name: "response", strict: true, schema } } } : {}),
        }),
    }).then((r) => r.json());
    if (response.error) return response;
    const content = response.choices?.[0]?.message?.content;
    try {
        return schema ? JSON.parse(content) : content;
    } catch (e) {
        return { error: e };
    }
}



function renderTable(data) {
    if (!data.length) return html`<p>No data.</p>`;
    const cols = Object.keys(data[0]);
    return html`
      <table class="table table-striped table-hover">
        <thead>
          <tr>
            ${cols.map((c) => html`<th>${c}</th>`)}
          </tr>
        </thead>
        <tbody>
          ${data.map((row) => html`
            <tr>
              ${cols.map((c) => html`<td>${row[c]}</td>`)}
            </tr>
          `)}
        </tbody>
      </table>
    `;
}

function download(content, filename, type) {
    const blob = new Blob([content], { type });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    link.click();
    URL.revokeObjectURL(url);
}

// --- Ticket Table Logic ---

const tableBody = document.getElementById('ticket-table-body');

// --- State Variables ---
let tableColumns = [];

let tickets = [];

function renderTicketTable(ticketList) {
    const tableBody = document.getElementById(`table-body`);
    const tableHead = document.getElementById(`table-head`);
    if (!tableBody || !tableHead) return;

    const headerRow = html`
        <tr>
            ${tableColumns.map(col => html`
                <th class="sortable" data-sort-col="${col}">${formatColumnName(col)}</th>
            `)}
        </tr>
    `;
    render(headerRow, tableHead);

    const rows = ticketList.map(ticket => {
        const trackTitle = ticket['Track Title'] ?? 'N/A';
        return html`
            <tr data-track-title="${trackTitle}">
                <td class="status-cell">
                    <span class="track-capsule status-pending">
                        <span class="status-dot dot-pending"></span>
                        ${trackTitle}
                    </span>
                </td>
                ${tableColumns.slice(1).map(col => html`<td>${ticket[col] ?? 'N/A'}</td>`)}
            </tr>
        `;
    });

    render(
        rows.length
            ? rows
            : html`<tr><td colspan="${tableColumns.length}" class="text-center p-5">No tickets found.</td></tr>`,
        tableBody
    );
}


import { unsafeHTML } from "https://cdn.jsdelivr.net/npm/lit-html@3/directives/unsafe-html.js";
let rawCSV = '';
let markdownTableRuleContext = '';
let completedspGroupedData = {}; // Global object to store DSP-grouped metadata


function databasecsv() {
    const rulesInput = document.getElementById('file');

    if (!rulesInput) {
        console.error("Rule file input not found.");
        return;
    }

    fetch('metadata_50.csv')
        .then(response => {
            if (!response.ok) throw new Error('rules.csv not found');
            return response.blob();
        })
        .then(blob => {
            const rulesFile = new File([blob], 'rules.csv', { type: 'text/csv' });
            const dataTransfer = new DataTransfer();
            dataTransfer.items.add(rulesFile);
            rulesInput.files = dataTransfer.files;
            rulesInput.dispatchEvent(new Event('change', { bubbles: true }));
            console.log("Rules uploaded successfully!");
        })
        .catch(error => {
            console.error("Rules Upload Error:", error);
            alert('Failed to upload rules.csv');
        });
}

databasecsv();

function uploadCSV() {
    const rulesInput = document.getElementById('rules-csv-input');

    if (!rulesInput) {
        console.error("Rule file input not found.");
        return;
    }

    fetch('rules.csv')
        .then(response => {
            if (!response.ok) throw new Error('rules.csv not found');
            return response.blob();
        })
        .then(blob => {
            const rulesFile = new File([blob], 'rules.csv', { type: 'text/csv' });
            const dataTransfer = new DataTransfer();
            dataTransfer.items.add(rulesFile);
            rulesInput.files = dataTransfer.files;
            rulesInput.dispatchEvent(new Event('change', { bubbles: true }));
            console.log("Rules uploaded successfully!");
        })
        .catch(error => {
            console.error("Rules Upload Error:", error);
            alert('Failed to upload rules.csv');
        });
}



function csvToMarkdownTable(csvData) {
    if (!Array.isArray(csvData) || !csvData.length) return '';

    const headers = Object.keys(csvData[0]);
    const separator = headers.map(() => "---");

    const numberedRows = csvData.map((row, index) => {
        const cells = headers.map(h => (row[h] || '').trim());
        return [index + 1, ...cells]; // Add Rule_No at start
    });

    const toRow = (cols) => `| ${cols.join(" | ")} |`;

    return [
        toRow(["Rule_No", ...headers]),
        toRow(["---", ...separator]),
        ...numberedRows.map(toRow)
    ].join("\n");
}



document.getElementById('rules-csv-input').addEventListener('change', async (e) => {
    const file = e.target.files[0];
    if (!file) return;

    const fileText = await file.text();

    const parsed = Papa.parse(fileText, {
        header: true,
        skipEmptyLines: true,
        dynamicTyping: true
    });

    rawCSV = parsed.data;
    markdownTableRuleContext = csvToMarkdownTable(rawCSV);

});

document.getElementById('check-status-button').addEventListener('click', async (e) => {
    collectValidationForTableRows('database');
})


function formatColumnName(col) {
    return col
        .replace(/_/g, " ")
        .replace(/\w\S*/g, (txt) => txt.charAt(0).toUpperCase() + txt.slice(1));
}

async function fetchTickets() {
    try {
        const tableName = db.exec("SELECT name FROM sqlite_master WHERE type='table'", { rowMode: "object" })[0].name;
        const sql = `SELECT * FROM "${tableName}"`;
        const allTickets = db.exec(sql, { rowMode: 'object' });

        // Derive column names dynamically
        tableColumns = Object.keys(allTickets[0] || {});

        tickets = db.exec(sql, { rowMode: 'object' });

        renderTicketTable(tickets);

        setupRowClickListener();
    } catch (err) {
        console.error("Error fetching tickets:", err);
        ['spotify', 'youtube', 'apple'].forEach(platform =>
            render(html`<tr><td colspan="99" class="text-center p-5 text-danger">Error: ${err.message}</td></tr>`, document.getElementById(`${platform}-table-body`))
        );
    }
}

document.addEventListener('click', function(event) {
    if (event.target && event.target.classList.contains('status-save')) {
        const button = event.target;
        const rowIndex = parseInt(button.getAttribute('data-row-index'), 10);
        const dsp = button.getAttribute('data-dsp');
        const ticketId = button.getAttribute('data-ticket-id');
        const row = button.closest('tr');

        const statusCellDetailed = Array.from(row.children).find(td => {
            return td.querySelector('.track-capsule') && td.textContent.toLowerCase().includes('failed');
        });

        if (statusCellDetailed) {
            statusCellDetailed.innerHTML = `
                <span class="track-capsule status-passed">
                    <span class="status-dot dot-passed"></span>
                    Passed
                </span>
            `;

            // Update the corresponding data in completedspGroupedData
            if (completedspGroupedData[dsp] && completedspGroupedData[dsp][ticketId] && completedspGroupedData[dsp][ticketId][rowIndex]) {
                completedspGroupedData[dsp][ticketId][rowIndex].status = 'Passed';
            }
        }

        // Make the new_value cell non-editable and display as plain text in detailed view
        const editableCellDetailed = row.querySelector('.editable-cell');
        if (editableCellDetailed) {
            const updatedText = editableCellDetailed.textContent.trim();
            editableCellDetailed.outerHTML = `<td>${updatedText}</td>`;

            // Update the corresponding data in completedspGroupedData
            if (completedspGroupedData[dsp] && completedspGroupedData[dsp][ticketId] && completedspGroupedData[dsp][ticketId][rowIndex]) {
                completedspGroupedData[dsp][ticketId][rowIndex].new_value = updatedText;
            }
        }

        // Remove Save button in detailed view
        button.closest('td').innerHTML = '';

        const dspWrapperDetailed = row.closest('.platform-heading-wrapper');
        const statusSpansDetailed = dspWrapperDetailed.querySelectorAll('td span.track-capsule');

        const allPassedDetailed = Array.from(statusSpansDetailed).every(span =>
            span.textContent.trim().toLowerCase() === 'passed'
        );

        if (allPassedDetailed) {
            const universalStatusDetailed = dspWrapperDetailed.querySelector('.universal-status');
            if (universalStatusDetailed) {
                universalStatusDetailed.classList.remove('status-failed');
                universalStatusDetailed.classList.add('status-passed');
                universalStatusDetailed.innerHTML = `
                    <span class="status-dot dot-passed"></span>
                    Passed
                `;
            }
        }

        // Update the status in the main table
        updateTrackStatusInMainTable(ticketId, !allPassedDetailed); // If all passed in detailed, then passed overall
    }
});

function updateTrackStatusInMainTable(trackTitle, hasFailed) {
    const row = document.querySelector(`tr[data-track-title="${trackTitle}"] .status-cell`);
    if (!row) return;

    const capsuleClass = hasFailed ? 'status-failed' : 'status-passed';
    const dotClass = hasFailed ? 'dot-failed' : 'dot-passed';

    row.innerHTML = `
        <span class="track-capsule ${capsuleClass}">
            <span class="status-dot ${dotClass}"></span>
            ${trackTitle}
        </span>
    `;
}


async function setupRowClickListener() {
    const metadataStatus = document.getElementById('metadataStatus');
    const rowDescription = document.getElementById('row-description');

    const tableBody = document.getElementById(`table-body`);
    if (!tableBody) return;

    tableBody.addEventListener('click', async (e) => {

        const td = e.target.closest('td');  
        const tr = e.target.closest('tr');
        if (!td || !tr) return;

        const cells = Array.from(tr.children);

        const clickedIndex = cells.indexOf(td);
        if (clickedIndex === -1) return;

        // Find ticket object
        const ticketId = td.textContent?.trim();
        if (!ticketId) return;

        let ticketList = tickets;

        const ticket = ticketList.find(t => t['Track Title']?.toString() === ticketId || t['Track Title']?.toString() === ticketId);
        if (!ticket) return;

        // Hide all tables
        const container = document.getElementById(`table-container`);
        if (container) container.style.display = 'none';

        // Show the row description panel
        if (rowDescription) rowDescription.style.display = 'block';
        document.getElementById('back-to-home-btn').style.display = 'none';
        renderMetadata(ticketId);
    });

    // Handle "Back to table" button
    const backBtn = document.getElementById('back-to-table');
    if (backBtn) {
        backBtn.addEventListener('click', () => {
            document.getElementById('back-to-home-btn').style.display = 'block';
            rowDescription.style.display = 'none';

            const container = document.getElementById(`table-container`);
            if (container) container.style.display = '';
        });
    }
}



function renderMetadata(ticketId, targetElementId = 'metadataStatus') {
    const targetEl = document.getElementById(targetElementId);
    if (!targetEl) {
        console.error(`Target element with ID ${targetElementId} not found.`);
        return;
    }

    if (Object.keys(completedspGroupedData).length === 0) {
        targetEl.innerHTML = '<p>Metadata not yet processed.</p>';
        return;
    }

    let tableHTML = '';

    for (const dsp in completedspGroupedData) {
        if (completedspGroupedData[dsp] && completedspGroupedData[dsp][ticketId]) {
            const platformIcon = getPlatformIcon(dsp);
            const rules = completedspGroupedData[dsp][ticketId];

            // Determine if all statuses are "passed" for this DSP and ticket
            const allPassed = rules.every(rule => rule.status?.toLowerCase() === 'passed');
            const statusClass = allPassed ? 'status-passed' : 'status-failed';
            const dotClass = allPassed ? 'dot-passed' : 'dot-failed';
            const statusText = allPassed ? 'Passed' : 'Failed';

            tableHTML += `
                <div class="platform-heading-wrapper" data-platform="${dsp}">
                    <h5 class="platform-heading">
                        <span class="platform-icon">${platformIcon} ${dsp}</span>
                        <span class="track-capsule universal-status ${statusClass}" data-dsp="${dsp}">
                            <span class="status-dot ${dotClass}"></span>
                            ${statusText}
                        </span>
                    </h5>
                    ${generateTableHTMLForTicket(rules,ticketId,dsp)}
                </div>
                `;
        }
    }

    if (tableHTML === '') {
        targetEl.innerHTML = `<p>No metadata found for ticket ID: ${ticketId}</p>`;
    } else {
        targetEl.innerHTML = tableHTML;
    }
}


function generateTableHTMLForTicket(rules,ticketId,dsp) {
    if (!rules || rules.length === 0) {
        return '<p>No rules found for this DSP.</p>';
    }
    const allColumns = Object.keys(rules[0]);
    let tableHTML = '<table class="table table-bordered table-striped"><thead><tr>';
    tableHTML += allColumns.map(col => `<th>${formatColumnName(col)}</th>`).join('');
    tableHTML += '<th>Save</th>';
    tableHTML += '</tr></thead><tbody>';

    rules.forEach((row, rowIndex) => {
        tableHTML += '<tr data-row-index="' + rowIndex + '">'; 
        let statusIsPassed = false;
        let currentDsp = ''; 
        let currentTicketId = ''; 
        if (row.dsp) {
            currentDsp = row.dsp;
        }else{
            currentDsp=dsp;
        }

        if (row.ticketId) {
            currentTicketId = row.ticketId;
        } else if (/* Logic to access ticketId from context */ ticketId) {
            currentTicketId = ticketId; 
        }


        allColumns.forEach(col => {
            const value = row[col] ?? 'N/A';

            if (col === 'status' && value !== 'N/A') {
                const statusClass = value.toLowerCase() === 'failed' ? 'status-failed' : 'status-passed';
                const dotClass = value.toLowerCase() === 'failed' ? 'dot-failed' : 'dot-passed';

                tableHTML += `
                    <td>
                        <span class="track-capsule ${statusClass}">
                            <span class="status-dot ${dotClass}"></span>
                            ${value}
                        </span>
                    </td>
                `;

                if (value.toLowerCase() === 'passed') {
                    statusIsPassed = true;
                }
            } else if (col === 'new_value') {
                if (!statusIsPassed) {
                    tableHTML += `
                        <td>
                            <span class="editable-cell"
                                contenteditable="true"
                                data-row-index="${rowIndex}"
                                data-column-name="${col}">
                                ${value}
                            </span>
                        </td>
                    `;
                } else {
                    tableHTML += `<td>${value}</td>`;
                }
            } else {
                tableHTML += `<td>${value}</td>`;
            }
        });

        if (!statusIsPassed) {
            tableHTML += `
                <td>
                    <button
                        class="track-capsule status-save"
                        data-row-index="${rowIndex}"
                        data-dsp="${currentDsp}"
                        data-ticket-id="${currentTicketId}"
                    >
                        Save
                    </button>
                </td>
            `;
        } else {
            tableHTML += `<td></td>`;
        }

        tableHTML += '</tr>';
    });

    tableHTML += '</tbody></table>';
    return tableHTML;
}



async function collectValidationForTableRows(tableName) {
    const results = { items: [] };
    completedspGroupedData = {}; // Initialize the global grouped data

    try {
        const rows = await db.exec(`SELECT * FROM [${tableName}]`, { rowMode: "object" });

        if (!rows.length) {
            console.warn(`No data found in table ${tableName}`);
            return results;
        }

        const batchSize = 1;
        const totalBatches = Math.ceil(rows.length / batchSize);
        const batchPromises = [];

        // Show the progress bar
        const progressContainer = document.getElementById('progress-container');
        const progressBar = document.getElementById('progress-bar');
        progressContainer.style.display = 'block';

        let completedBatches = 0;

        for (let i = 0; i < rows.length; i += batchSize) {
            const batch = rows.slice(i, i + batchSize);

            batchPromises.push((async () => {
                try {
                    const batchResponse = await getLLMValidationResponse(batch);
                    if (Array.isArray(batchResponse)) {
                        results.items.push(...batchResponse);

                        // Process and group the data immediately
                        for (const item of batchResponse) {
                            const processedData = processIndividualItem(rawCSV, item);
                            const groupedData = divideIndividualItemByDsp(processedData);

                            // Merge into the global grouped data
                            for (const dsp in groupedData) {
                                if (!completedspGroupedData[dsp]) {
                                    completedspGroupedData[dsp] = {};
                                }
                                if (!completedspGroupedData[dsp][item['Track Title']]) {
                                    completedspGroupedData[dsp][item['Track Title']] = [];
                                }
                                completedspGroupedData[dsp][item['Track Title']].push(...groupedData[dsp]);
                            }

                            const hasFailedRules = item.rules.some(rule => rule.status === 'Failed');
                            updateTrackStatusInMainTable(item['Track Title'], hasFailedRules);
                        }
                    } else {
                        console.warn(`Invalid response for batch ${i}-${i + batch.length}`);
                    }
                } catch (err) {
                    console.error(`Batch ${i}-${i + batch.length} failed:`, err);
                }

                // Update progress bar
                completedBatches++;
                const progress = Math.round((completedBatches / totalBatches) * 100);
                progressBar.style.width = `${progress}%`;
                progressBar.setAttribute('aria-valuenow', progress);
                progressBar.textContent = `${progress}%`;
            })());
        }

        await Promise.all(batchPromises);

        progressContainer.style.display = 'none';

        return results;

    } catch (err) {
        console.error("Validation Collection Error:", err);
        return results;
    }
}

function processIndividualItem(rawCSV, item) {
    const headers = Object.keys(rawCSV[0] || {});
    const additionalColumns = ['status', 'reason', 'suggestion', 'new_value'];
    const allColumns = [...headers, ...additionalColumns];
    const tableData = [];
    const ticketId = item['Track Title'];

    rawCSV.forEach((csvRow, index) => {
        const ruleNo = (index + 1).toString();
        let matchedRule = null;

        if (Array.isArray(item.rules)) {
            matchedRule = item.rules.find(r => r.rule_no?.toString() === ruleNo);
        }

        const rowData = {
            ...csvRow,
            status: matchedRule ? matchedRule.status ?? '' : '',
            reason: matchedRule ? matchedRule.reason ?? '' : '',
            suggestion: matchedRule ? matchedRule.suggestion ?? '' : '',
            new_value: matchedRule ? matchedRule.new_value ?? '' : ''
        };
        tableData.push(rowData);
    });
    return tableData;
}

function divideIndividualItemByDsp(tableData) {
    const dspGroupedData = {};

    tableData.forEach(row => {
        const dsp = row.DSP;
        if (!dsp) return;
        if (!dspGroupedData[dsp]) {
            dspGroupedData[dsp] = [];
        }
        dspGroupedData[dsp].push(row);
    });
    return dspGroupedData;
}


function getPlatformIcon(platform) {
  const iconMap = {
    spotify: '<i class="fab fa-spotify text-success"></i>',           // Green Spotify icon
    youtube: '<i class="fab fa-youtube text-danger"></i>',           // Red YouTube icon
    'apple music': '<i class="fab fa-apple text-dark"></i>',         // Apple icon for Apple Music
  };

  return iconMap[platform.toLowerCase()] || ''; // Return icon HTML or empty string
}


async function getLLMValidationResponse(batchData) {
    try {
        if (!markdownTableRuleContext) {
            throw new Error("Rule context not loaded.");
        }

        const systemPrompt = `
You are Rule checker agent, a highly accurate and consistent metadata validation engine designed for digital music platforms to validation against each and every Rule given to you in Rules table.

You receive:
- A table of platform-specific rules (in Markdown format),
- A batch of track metadata (as JSON objects — one per track).

Your task:
- For **each track in the batch**, validate its fields against all applicable rules based on platform (e.g., Spotify, YouTube, Apple Music).
- For each applicable rule:
    - If the track passes the rule, include \`status: "Passed"\`.
    - If the track **fails** the rule, include:
        - \`rule_no\`: A unique ID (1, 2, etc),
        - \`platform\`: Name of the DSP,
        - \`status: "Failed"\`,
        - \`reason\`: Why it failed,
        - \`suggestion\`: How to fix it,
        - \`new_value\`: Suggested corrected value (if applicable).

Return a **JSON array**, where each object corresponds to **one track** and includes:
- \`Track Title\`
- A \`rules\` array (list of all rule evaluations for that track)

Make sure to process **all tracks** in the input batch and return validation for **each**.
Respond strictly using the format shown below.

You have to cover each and rules status in the below output json format. Do not leave any rule.

Example output:
\`\`\`json
[
  {
    "Track Title": "Track 1",
    "rules": [
      {
        "rule_no": "1",
        "platform": "Spotify",
        "status": "Failed",
        "reason": "Format is MP3, not WAV/FLAC",
        "suggestion": "Use 16-bit or 24-bit WAV/FLAC",
        "new_value": "24-bit WAV"
      },
      {
        "rule_no": "2",
        "platform": "Spotify",
        "status": "Passed"
      }
    ]
  },
  {
    "Track Title": "Track 2",
    "rules": [
      {
        "rule_no": "1",
        "platform": "Apple Music",
        "status": "Passed"
      }
    ]
  }
]
\`\`\`

Validation Rules Table:

${markdownTableRuleContext}`;

        const userPrompt = `
Here is a batch of track metadata in JSON format. Evaluate each track individually and return the validation result for each rule in correct order from rule 1 to rule 42.
Special Note: Always Ensure you mention about all rules in your output.

\`\`\`json
${JSON.stringify(batchData, null, 2)}
\`\`\`
        `.trim();

        const response = await llm({
            system: systemPrompt,
            user: userPrompt
        });

        const jsonMatch = response.match(/```json([\s\S]*?)```/);
        if (jsonMatch && jsonMatch[1]) {
            const jsonText = jsonMatch[1].trim();

            // Parse the JSON content
            const parsed = JSON.parse(jsonText);

            if (!Array.isArray(parsed)) {
                throw new Error("LLM did not return an array");
            }
            return parsed;
        } else {
            throw new Error("JSON response not found");
        }

    } catch (err) {
        console.error("LLM Error:", err);
        console.log("danger", "LLM Error", `Failed to process batch: ${err.message}`);
        return null;
    }
}

// ------------------ Draw Tables & Column UI ------------------
async function drawTables() {
    const schema = DB.schema();
    if (!schema.length) {
        render(html`<p>No tables available.</p>`, $tablesContainer);
        return;
    }
    const content = html`
      <div class="accordion narrative mx-auto" id="table-accordion">
        ${schema.map(({ name, sql, columns }) => {
        return html`
            <div class="accordion-item my-2">
              <h2 class="accordion-header">
                <button
                  class="accordion-button collapsed"
                  type="button"
                  data-bs-toggle="collapse"
                  data-bs-target="#collapse-${name}"
                  aria-expanded="false"
                  aria-controls="collapse-${name}"
                >
                  ${name}
                </button>
              </h2>
              <div
                id="collapse-${name}"
                class="accordion-collapse collapse"
                data-bs-parent="#table-accordion"
              >
                <div class="accordion-body">
                  <pre style="white-space: pre-wrap">${sql}</pre>
                  <!-- Table of columns -->
                  
                </div>
              </div>
            </div>
          `;
    })}
      </div>
      <!-- Query form -->
      <form class="narrative mx-auto " id="question-form">
        <div class="mb-3 d-none">
          <label for="context" class="form-label fw-bold">
            Provide context about your dataset:
          </label>
          <textarea class="form-control" name="context" id="context" rows="3">
  ${DB.context}</textarea>
        </div>
        <div class="mb-3 d-flex align-items-center">
  <textarea class="form-control me-2" name="query" id="query" rows="1" placeholder="Ask a question"></textarea>
  <button type="submit" class="btn btn-primary">Submit</button>
</div>

      </div>
      </form>
    `;
    render(content, $tablesContainer);
    const $forms = $tablesContainer.querySelectorAll("form");
    $forms.forEach(($form) => {
        if ($form.id === "question-form") {
            $form.addEventListener("submit", onQuerySubmit);
        }
    });
}

// ------------------ Query Form Submission ------------------
async function onQuerySubmit(e) {
    e.preventDefault();
    try {
        const formData = new FormData(e.target);
        const query = formData.get("query");
        DB.context = formData.get("context") || "";
        render("", $result);

        const result = await llm({
            system: `You are an expert SQLite query writer. The user has a SQLite dataset.
  
  ${DB.context}
  
  The schema is:
  
  ${DB.schema().map(({ sql }) => sql).join("\n\n")}
  
  Answer the user's question by describing steps, then output final SQL code (SQLite).`,
            user: query,
        });
        // render(html`${unsafeHTML(marked.parse(result))}`, $sql);

        const sqlCode = result.match(/```.*?\n([\s\S]*?)```/);
        const extractedSQL = sqlCode ? sqlCode[1] : result;
        queryHistory.push("Main Query:\n" + extractedSQL);
        try {
            const rows = db.exec(extractedSQL, { rowMode: "object" });
            if (rows.length > 0) {
                latestQueryResult = rows;

                render(html`
        
          <div style="padding: 10px; max-height:60%; overflow-y: auto;"">
            ${renderTable(rows.slice(0, 100))}
          </div>
        <div class="accordion mt-3" id="resultAccordion">
          <div class="accordion-item">
            <h2 class="accordion-header">
              <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#summaryCollapse">
                Download Results
              </button>
            </h2>
            <div id="summaryCollapse" class="accordion-collapse collapse">
              <div class="accordion-body">
                <button id="download-csv" class="btn-plain">
                  <i class="bi bi-filetype-csv"></i> Download CSV
                </button>
              </div>
            </div>
          </div>
          
        </div>
      `, $result);
                document.getElementById("download-csv").addEventListener("click", () => {
                    download(dsvFormat(",").format(latestQueryResult), "datachat.csv", "text/csv");
                });
            }
            else {
                render(html`<p>No results found.</p>`, $result);
            }
        } catch (err) {
            render(html`<div class="alert alert-danger">${err.message}</div>`, $result);
        }
    } finally {
        console.log();
    }
}

