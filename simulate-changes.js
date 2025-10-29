// simulate-changes.js
const { MongoClient } = require('mongodb');

async function simulateDataChanges() {
    const client = new MongoClient("mongodb://localhost:27017/");
    
    try {
        await client.connect();
        const database = client.db('DB1');
        const salesCollection = database.collection('Sales Header');
        
        console.log('🎮 Starting data change simulation...');
        console.log('Press Ctrl+C to stop\n');
        
        let counter = 1;
        
        // Add a new record every 10 seconds
        setInterval(async () => {
            const newSale = {
                sale_id: `SIM${Date.now()}`,
                customer_id: `CUST${Math.floor(Math.random() * 1000)}`,
                amount: Math.floor(Math.random() * 1000) + 100,
                product: `Product${Math.floor(Math.random() * 10) + 1}`,
                timestamp: new Date(),
                simulated: true,
                batch: counter
            };
            
            await salesCollection.insertOne(newSale);
            console.log(`📝 Added simulated sale #${counter}: ${newSale.sale_id}`);
            counter++;
            
        }, 10000); // Every 10 seconds
        
    } catch (error) {
        console.error('Simulation error:', error);
    }
}

simulateDataChanges();
