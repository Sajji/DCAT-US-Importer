const fs = require("fs");
const path = require("path");
const https = require('https');

//**********CONFIGURATION************
const dataDirectory = "./dataDownloads"; // Directory containing your source JSON files
const collibraHostname = 'fedworld.collibra.com'; 
const collibraPort = 443; 
const collibraApiPath = '/rest/2.0/import/json-job'; 
const username = 'USERNAME'; // Replace with your Collibra username
const password = 'PASSWORD'; // Replace with your Collibra password
//***********************************

function transformData(data, comName) {
    // Build base community and domain objects
    const domainStructure = [
        {
            "resourceType": "Community",
            "identifier": {
                "name": comName,
            },
        },
        {
            "resourceType": "Domain",
            "identifier": {
                "name": `${comName}-Domain`,
                "community": {
                    "name": comName,
                },
            },
            "type": {
                "name": "Data Usage Registry",
            },
        },
    ];

    // Transform each dataset to Collibra-friendly assets
    const datasets = data.dataset || [];
    const transformedAssets = datasets.map((d) => {
        const downloadURL =
            Array.isArray(d.distribution) && d.distribution.length > 0
                ? d.distribution[0].downloadURL || ""
                : "";

        const keywords = Array.isArray(d.keyword)
            ? `Keyword: ${d.keyword.map((k) => `"${k}"`).join(", ")}`
            : "Keyword: ";

        const contactEmail = d.contactPoint?.hasEmail?.replace("mailto:", "") || "";

        return {
            "resourceType": "Asset",
            "identifier": {
                "name": `${d.title}---${d.identifier}`,
                "domain": {
                    "name": `${comName}-Domain`,
                    "community": {
                        "name": comName,
                    },
                },
            },
            "type": {
                "name": "DCAT Data Set",
            },
            "attributes": {
                "Description": [{"value": `Description: ${d.description}`}],
                "DCAT:title": [{"value": `Title: ${d.title}`}],
                "DCAT:landingPage": [{"value": `Landing Page: ${d.landingPage || ""}`}],
                "DCAT:description": [{"value": `Description: ${d.description}`}],
                "DCAT:accessLevel": [{"value": `Access Level: ${d.accessLevel}`}],
                "DCAT:distributionURL": [{"value": `Distribution URL: ${downloadURL || "None Provided"}`}],
                "DCAT:modified": [{"value": `Modified: ${d.modified || ""}`}],
                "DCAT:spatial": [{"value": `Spatial: ${d.spatial || "None Provided"}`}],
                "DCAT:license": [{"value": `License: ${d.license || ""}`}],
                "DCAT:contactPointFullName": [
                    {"value": `Contact: ${d.contactPoint?.fn || ""}`},
                ],
                "DCAT:contactPointEmail": [
                    {"value": `Contact Email: ${contactEmail}`},
                ],
                "DCAT:programCode": [
                    {
                        "value": `Program Code: ${
                            Array.isArray(d.programCode) ? d.programCode[0] : ""
                        }`,
                    },
                ],
                "DCAT:bureauCode": [
                    {
                        "value": `Bureau Code: ${
                            Array.isArray(d.bureauCode) ? d.bureauCode[0] : ""
                        }`,
                    },
                ],
                "DCAT:publisher": [
                    {"value": `Published By: ${d.publisher?.name || ""}`},
                ],
                "DCAT:keyword": [{"value": keywords}],
                // "DCAT:distribution": [{"value": `Distribution: ${d.distribution || "None Provided"}`}],
            },
        };
    });

    // Combine everything and return as a string
    return JSON.stringify([...domainStructure, ...transformedAssets], null, 2);
}

function sendToCollibra(fileContent, comName) {
    const boundary = '----WebKitFormBoundary7MA4YWxkTrZu0gW'; // A random boundary string
    
    // Construct the multipart/form-data body
    let postData = `--${boundary}\r\n`;
    postData += `Content-Disposition: form-data; name="continueOnError"\r\n\r\n`;
    postData += `false\r\n`;
    postData += `--${boundary}\r\n`;
    postData += `Content-Disposition: form-data; name="deleteFile"\r\n\r\n`;
    postData += `false\r\n`;
    postData += `--${boundary}\r\n`;
    postData += `Content-Disposition: form-data; name="simulation"\r\n\r\n`;
    postData += `false\r\n`;
    postData += `--${boundary}\r\n`;
    postData += `Content-Disposition: form-data; name="fileName"\r\n\r\n`;
    postData += `import_file\r\n`;
    postData += `--${boundary}\r\n`;
    postData += `Content-Disposition: form-data; name="relationsAction"\r\n\r\n`;
    postData += `ADD_OR_IGNORE\r\n`;
    postData += `--${boundary}\r\n`;
    postData += `Content-Disposition: form-data; name="sendNotification"\r\n\r\n`;
    postData += `false\r\n`;
    postData += `--${boundary}\r\n`;
    postData += `Content-Disposition: form-data; name="fileId"\r\n\r\n`;
    postData += `\r\n`;
    postData += `--${boundary}\r\n`;
    postData += `Content-Disposition: form-data; name="saveResult"\r\n\r\n`;
    postData += `false\r\n`;
    postData += `--${boundary}\r\n`;
    postData += `Content-Disposition: form-data; name="batchSize"\r\n\r\n`;
    postData += `1000\r\n`;
    postData += `--${boundary}\r\n`;
    postData += `Content-Disposition: form-data; name="file"; filename="${comName}.json"\r\n`;
    postData += `Content-Type: application/json\r\n\r\n`;
    postData += fileContent;
    postData += `\r\n--${boundary}--\r\n`;

    const auth = 'Basic ' + Buffer.from(username + ':' + password).toString('base64');

    const options = {
        hostname: collibraHostname,
        port: collibraPort,
        path: collibraApiPath,
        method: 'POST',
        headers: {
            'Content-Type': `multipart/form-data; boundary=${boundary}`,
            'Content-Length': Buffer.byteLength(postData),
            'Authorization': auth,
        }
    };

    const req = https.request(options, (res) => {
        console.log(`STATUS for ${comName}: ${res.statusCode}`);
        res.setEncoding('utf8');
        let responseBody = '';
        res.on('data', (chunk) => {
            responseBody += chunk;
        });
        res.on('end', () => {
            console.log(`RESPONSE BODY for ${comName}:`, responseBody);
        });
    });

    req.on('error', (e) => {
        console.error(`problem with request for ${comName}: ${e.message}`);
    });

    req.write(postData);
    req.end();
}

// Main execution logic
fs.readdir(dataDirectory, (err, files) => {
    if (err) {
        console.error("Error reading directory:", err);
        return;
    }

    files.forEach(file => {
        if (path.extname(file) === '.json') {
            const filePath = path.join(dataDirectory, file);
            const comName = path.basename(file, '.json');
            
            try {
                const fileContent = fs.readFileSync(filePath, 'utf8');
                const parsedData = JSON.parse(fileContent);
                const finalData = transformData(parsedData, comName);
                sendToCollibra(finalData, comName);
            } catch (error) {
                console.error(`Error processing file ${file}:`, error);
            }
        }
    });

});
