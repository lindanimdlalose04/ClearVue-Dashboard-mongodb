const express = require('express');
const { MongoClient } = require('mongodb');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname)));

const connectionString = "mongodb://localhost:27017/";

// API endpoint to get list of collections
app.get('/api/collections', async (req, res) => {
    let client;
    try {
        client = new MongoClient(connectionString);
        await client.connect();
        const database = client.db('DB1');
        const collections = await database.listCollections().toArray();
        
        res.json({
            collections: collections.map(c => c.name),
            totalCollections: collections.length
        });
        
    } catch (error) {
        res.status(500).json({ error: error.message });
    } finally {
        if (client) await client.close();
    }
});

// API endpoint to get data from specific collection
app.get('/api/data/:collectionName', async (req, res) => {
    let client;
    try {
        client = new MongoClient(connectionString);
        await client.connect();
        
        const database = client.db('DB1');
        const collectionName = req.params.collectionName;
        const collection = database.collection(collectionName);
        
        // Get all documents from the collection
        const data = await collection.find({}).toArray();
        
        console.log(`📊 Found ${data.length} documents in ${collectionName}`);
        res.json({
            collection: collectionName,
            count: data.length,
            data: data
        });
        
    } catch (error) {
        console.error('❌ Error:', error);
        res.status(500).json({ error: error.message });
    } finally {
        if (client) await client.close();
    }
});

// Get sample data from multiple important collections
app.get('/api/dashboard-data', async (req, res) => {
    let client;
    try {
        client = new MongoClient(connectionString);
        await client.connect();
        const database = client.db('DB1');
        
        // Get data from key collections for the dashboard
        const [customers, products, sales, purchases] = await Promise.all([
            database.collection('Customer').find({}).limit(20).toArray(),
            database.collection('Products').find({}).limit(20).toArray(),
            database.collection('Sales Header').find({}).limit(20).toArray(),
            database.collection('Purchases Headers').find({}).limit(20).toArray()
        ]);
        
        res.json({
            customers: { count: customers.length, data: customers },
            products: { count: products.length, data: products },
            sales: { count: sales.length, data: sales },
            purchases: { count: purchases.length, data: purchases }
        });
        
    } catch (error) {
        res.status(500).json({ error: error.message });
    } finally {
        if (client) await client.close();
    }
});

// Real-time sales data stream
app.get('/api/stream/sales', async (req, res) => {
    // Set headers for Server-Sent Events
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*'
    });
    
    console.log('🎯 Client connected to Sales Data Stream');
    
    let client;
    try {
        client = new MongoClient(connectionString);
        await client.connect();
        const database = client.db('DB1');
        const salesCollection = database.collection('Sales Header');
        
        // Get initial count
        let lastCount = await salesCollection.countDocuments();
        
        // Send initial data
        const initialData = await salesCollection.find({})
            .sort({ _id: -1 })
            .limit(10)
            .toArray();
            
        res.write(`data: ${JSON.stringify({
            type: 'initial',
            message: 'Sales data stream started',
            timestamp: new Date(),
            record_count: lastCount,
            data: initialData
        })}\n\n`);
        
        // Check for changes every 3 seconds
        const interval = setInterval(async () => {
            try {
                const currentCount = await salesCollection.countDocuments();
                
                if (currentCount !== lastCount) {
                    console.log('🔄 Data change detected in Sales Header');
                    
                    const newData = await salesCollection.find({})
                        .sort({ _id: -1 })
                        .limit(5)
                        .toArray();
                    
                    res.write(`data: ${JSON.stringify({
                        type: 'update',
                        timestamp: new Date(),
                        record_count: currentCount,
                        new_records: newData,
                        message: `Data updated: ${currentCount} records total`
                    })}\n\n`);
                    
                    lastCount = currentCount;
                } else {
                    // Send heartbeat to keep connection alive
                    res.write(`data: ${JSON.stringify({
                        type: 'heartbeat',
                        timestamp: new Date(),
                        message: 'Stream active'
                    })}\n\n`);
                }
            } catch (error) {
                console.error('Stream error:', error);
                res.write(`data: ${JSON.stringify({
                    type: 'error',
                    error: error.message
                })}\n\n`);
            }
        }, 3000); // Check every 3 seconds
        
        // Clean up when client disconnects
        req.on('close', () => {
            clearInterval(interval);
            if (client) {
                client.close();
            }
            console.log('🎯 Client disconnected from Sales Data Stream');
        });
        
    } catch (error) {
        console.error('Stream setup error:', error);
        res.write(`data: ${JSON.stringify({
            type: 'error',
            error: 'Failed to setup stream'
        })}\n\n`);
        res.end();
    }
});

// Real-time customer data stream
app.get('/api/stream/customers', async (req, res) => {
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*'
    });
    
    console.log('🎯 Client connected to Customer Data Stream');
    
    let client;
    try {
        client = new MongoClient(connectionString);
        await client.connect();
        const database = client.db('DB1');
        const customerCollection = database.collection('Customer');
        
        let lastCount = await customerCollection.countDocuments();
        
        // Send heartbeat every 5 seconds
        const interval = setInterval(async () => {
            try {
                const currentCount = await customerCollection.countDocuments();
                
                if (currentCount !== lastCount) {
                    console.log('🔄 Data change detected in Customer');
                    
                    const newData = await customerCollection.find({})
                        .sort({ _id: -1 })
                        .limit(3)
                        .toArray();
                    
                    res.write(`data: ${JSON.stringify({
                        type: 'update',
                        timestamp: new Date(),
                        record_count: currentCount,
                        new_records: newData,
                        message: `Customer data updated`
                    })}\n\n`);
                    
                    lastCount = currentCount;
                } else {
                    res.write(`data: ${JSON.stringify({
                        type: 'heartbeat',
                        timestamp: new Date(),
                        record_count: currentCount,
                        message: 'Customer stream active'
                    })}\n\n`);
                }
            } catch (error) {
                console.error('Customer stream error:', error);
            }
        }, 5000);
        
        req.on('close', () => {
            clearInterval(interval);
            if (client) client.close();
            console.log('🎯 Client disconnected from Customer Data Stream');
        });
        
    } catch (error) {
        console.error('Customer stream setup error:', error);
        res.end();
    }
});

// Clean flat data for Power BI
app.get('/api/powerbi/:collectionName', async (req, res) => {
    let client;
    try {
        client = new MongoClient(connectionString);
        await client.connect();
        
        const database = client.db('DB1');
        const collection = database.collection(req.params.collectionName);
        
        const data = await collection.find({}).toArray();
        
        // Convert to clean flat data
        const cleanData = data.map(item => {
            const cleanItem = {};
            for (const [key, value] of Object.entries(item)) {
                if (key === '_id') {
                    cleanItem[key] = value.toString();
                } else if (value && typeof value === 'object') {
                    if (value.$numberInt) {
                        cleanItem[key] = parseInt(value.$numberInt);
                    } else if (value.$numberDouble) {
                        cleanItem[key] = parseFloat(value.$numberDouble);
                    } else if (value.$date) {
                        cleanItem[key] = new Date(parseInt(value.$date.$numberLong));
                    } else {
                        cleanItem[key] = JSON.stringify(value);
                    }
                } else {
                    cleanItem[key] = value;
                }
            }
            return cleanItem;
        });
        
        res.json(cleanData);
        
    } catch (error) {
        res.status(500).json({ error: error.message });
    } finally {
        if (client) await client.close();
    }
});

const PORT = 3000;
app.listen(PORT, () => {
    console.log(`🎯 Server running on http://localhost:${3000}`);
    console.log(`📊 Dashboard: http://localhost:${3000}`);
    console.log(`📁 Collections API: http://localhost:${3000}/api/collections`);
    console.log(`🔥 REAL-TIME STREAMS:`);
    console.log(`   📈 Sales: http://localhost:${3000}/api/stream/sales`);
    console.log(`   👥 Customers: http://localhost:${3000}/api/stream/customers`);
});
