const admin = require("firebase-admin");
const serviceAccount = require("./admin.json");
const Web3 = require('web3');
const abi = require('./abi.json')
const { promisify } = require('util');

admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: "https://aave-analytics-default-rtdb.firebaseio.com/"
});
const db = admin.database();
let add = 'wss://4b5a3125fc584c25b7099611205eeeda.eth.ws.rivet.cloud/'
const web3 = new Web3(new Web3.providers.WebsocketProvider(add,
    {
        clientConfig: {
            maxReceivedFrameSize: 1000000000,
            maxReceivedMessageSize: 1000000000,
        }
    }));

async function getBorrowers(address, startBlock, count, snapshot) {
    const poolContract = new web3.eth.Contract(abi.poolAbi, '0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9');
    const currentBlock = await web3.eth.getBlockNumber();
    const contract = new web3.eth.Contract(abi.erc20, address)
    const symbol = (await contract.methods.symbol().call()).replace(/[^A-Z0-9]+/gi, '');
    const dec = await contract.methods.decimals().call()
    const addressesSearched = []
    const addressRef = db.ref(`aave/pools/0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9/addresses`);
    const symbolRef = db.ref(`aave/pools/0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9/tokenHolders/${symbol}`);
    const tokenRef = db.ref(`aave/pools/0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9/tokenHolders/${symbol}/addresses`);
    var runningJobs = 0;
    const getPastEvents = promisify(contract.getPastEvents.bind(contract));

    async function updateAccount(address) {
        const accountData = await poolContract.methods.getUserAccountData(address).call()
        await addressRef.child(address).update({
            totalCollateralETH: parseInt(accountData.totalCollateralETH) / (10 ** 18),
            totalDebtETH: parseInt(accountData.totalDebtETH) / (10 ** 18),
            availableBorrowsETH: parseInt(accountData.availableBorrowsETH) / (10 ** 18),
            currentLiquidationThaccountDatahold: parseInt(accountData.currentLiquidationThreshold) / 100,
            ltv: parseInt(accountData.ltv) / 100,
            healthFactor: parseInt(accountData.healthFactor)
        })
    }

    async function checkAccount(address) {
        const balance = await contract.methods.balanceOf(address).call();
        const balanceAdjusted = parseInt(balance) / 10 ** dec;
        const exists = (!!(snapshot && snapshot.tokenHolders && snapshot.tokenHolders[symbol]
            && snapshot.tokenHolders[symbol].addresses && snapshot.tokenHolders[symbol].addresses[address] !== null
            && snapshot.tokenHolders[symbol].addresses[address] !== undefined))
        if (parseInt(balance) !== 0) {
            if (!exists) { count++ }
            if (!exists || (!snapshot.tokenHolders[symbol].addresses[address] === balanceAdjusted)) {
                await tokenRef.update({
                    [address]: balanceAdjusted,
                });
                await addressRef.child(address + "/tokens").update({
                    [symbol]: balanceAdjusted,
                });
                await updateAccount(address)
            }
        } else {
            if (exists) {
                await tokenRef.child(`${address}`).remove();
                await addressRef.child(`${address}/tokens/${symbol}`).remove();
                await updateAccount(address)
                console.log("Removed " + address + " " + symbol)
                count--;
            }
        }
    }

    async function readEventsRange(start, end) {
        runningJobs += 1
        console.log("Event " + start + " to " + end + " for " + symbol)
        try {
            const events = await getPastEvents('Transfer', { fromBlock: start, toBlock: end });
            if (events.length > 0) {
                await Promise.all(
                    events
                        .filter(
                            (e) =>
                                (e.returnValues.from !== '0x0000000000000000000000000000000000000000' &&
                                    !addressesSearched.includes(e.returnValues.from)) || (e.returnValues.to !== '0x0000000000000000000000000000000000000000' &&
                                        !addressesSearched.includes(e.returnValues.to))
                        )
                        .map(async (e) => {
                            if (e.returnValues.from !== '0x0000000000000000000000000000000000000000' &&
                                !addressesSearched.includes(e.returnValues.from)) {
                                addressesSearched.push(e.returnValues.from);
                                await checkAccount(e.returnValues.from)
                            }
                            if (e.returnValues.to !== '0x0000000000000000000000000000000000000000' &&
                                !addressesSearched.includes(e.returnValues.to)) {
                                addressesSearched.push(e.returnValues.to);
                                await checkAccount(e.returnValues.to)
                            }
                        }),
                );
                runningJobs -= 1;
                if (runningJobs === 0) {
                    console.log("DONE " + symbol)
                    await symbolRef.update({
                        time: (new Date()).toUTCString(),
                        lastBlock: currentBlock,
                        count: count
                    });
                    console.log("Finished " + symbol)
                }
            } else {
                runningJobs -= 1;
                if (runningJobs === 0) {
                    await symbolRef.update({
                        time: (new Date()).toUTCString(),
                        lastBlock: currentBlock,
                        count: count
                    });
                    console.log("Finished " + symbol)
                }
            }
        } catch (errors) {
            console.log(errors.message)
            if (errors.message.includes('10,000 results')) {
                const middle = Math.round((start + end) / 2);
                runningJobs -= 1;
                return Promise.all([
                    readEventsRange(start, middle),
                    readEventsRange(middle + 1, end),
                ]);
            }
            else {
                await symbolRef.update({
                    time: (new Date()).toUTCString(),
                    count: count
                });
                console.log("Error - updating count")
                return;
            }
        }
    }
    return readEventsRange(startBlock, currentBlock)
}

exports.helloPubSub = (event, context) => {
    const ref = db.ref("aave/pools/0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9")
    return new Promise(function (resolve, reject) {
        ref.once('value', async (snapshot) => {
            const promises = []
            for (let obj of Object.values(snapshot.val().tokens)) {
                const tokenExists = snapshot.val() && snapshot.val().tokenHolders && snapshot.val().tokenHolders[obj.aTokenSymbol]
                const start = tokenExists && snapshot.val().tokenHolders[obj.aTokenSymbol].lastBlock
                    ? snapshot.val().tokenHolders[obj.aTokenSymbol].lastBlock : 0
                const count = tokenExists && snapshot.val().tokenHolders[obj.aTokenSymbol].count
                    ? snapshot.val().tokenHolders[obj.aTokenSymbol].count : 0
                promises.push(getBorrowers(obj.aTokenAddress, start, count, snapshot.val()))
            }
            Promise.all(promises).then(() => {
                console.log("DONE")
                resolve();
            })
        }, (errorObject) => {
            console.log('The read failed: ' + errorObject.name);
        });
    })

};