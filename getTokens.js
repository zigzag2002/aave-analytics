/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */

var admin = require("firebase-admin");
var serviceAccount = require("./admin.json");

const Web3 = require('web3');
const abi = require('./abi.json')

admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: "https://aave-analytics-default-rtdb.firebaseio.com/"
});

var db = admin.database();

// initialize eth node
let add = 'wss://4b5a3125fc584c25b7099611205eeeda.eth.ws.rivet.cloud/'
var web3 = new Web3(new Web3.providers.WebsocketProvider(add,
    {
        clientConfig: {
            maxReceivedFrameSize: 1000000000,
            maxReceivedMessageSize: 1000000000,
        }
    }));



async function getPoolData(address) {
    var tokenRef = db.ref(`aave/pools/${address}/tokens`);
    const poolContract = new web3.eth.Contract(abi.poolAbi, address);
    const poolAddressProvider = new web3.eth.Contract(abi.providerAbi, '0xb53c1a33016b2dc2ff3653530bff1848a515c8c5')
    const priceOracleAddress = await poolAddressProvider.methods.getPriceOracle().call()
    const priceOracleContract = new web3.eth.Contract(abi.priceOracle, priceOracleAddress)
    const list = await poolContract.methods.getReservesList().call();
    for (let address of list) {
        var assetContract;
        var assetSymbol;
        var assetName;
        if (address === '0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2') {
            assetContract = new web3.eth.Contract(abi.makerToken, address);
            assetSymbol = web3.utils.hexToAscii(await assetContract.methods.symbol().call());
            assetName = web3.utils.hexToAscii(await assetContract.methods.name().call());
        }

        else {
            assetContract = new web3.eth.Contract(abi.erc20, address);
            assetSymbol = await assetContract.methods.symbol().call();
            assetName = await assetContract.methods.name().call();
        }
        const price = (await priceOracleContract.methods.getAssetPrice(address).call()) / (10**18);
        const assetConfiguration = await poolContract.methods.getConfiguration(address).call();
        const num = BigInt(assetConfiguration[0])
        const binary = num.toString(2)
        const strlen = binary.length
        const loanToValue = parseInt(binary.substring(strlen - 16, strlen), 2) / 100
        const liquidationThreshold = parseInt(binary.substring(strlen - 32, strlen - 16), 2) / 100
        const liquidationBonus = parseInt(binary.substring(strlen - 48, strlen - 32), 2) / 100
        const decimals = parseInt(binary.substring(strlen - 56, strlen - 48), 2)
        const reserveIsActive = (parseInt(binary[strlen - 57]) === 1)
        const reserveIsFrozen = (parseInt(binary[strlen - 58]) === 1)
        const borrowingIsEnabled = (parseInt(binary[strlen - 59]) === 1)
        const stableRateBorrowingEnabled = (parseInt(binary[strlen - 60]) === 1)
        var reserved = parseInt(binary.substring(strlen - 64, strlen - 60), 2)
        var reserveFactor = parseInt(binary.substring(0, strlen - 64), 2) / 100

        if (isNaN(reserved)) { reserved = 0 }
        if (isNaN(reserveFactor)) { reserveFactor = 0 }

        const assetData = await poolContract.methods.getReserveData(address).call();
        const aTokenContract = new web3.eth.Contract(abi.erc20, assetData.aTokenAddress);
        const stableTokenContract = new web3.eth.Contract(abi.erc20, assetData.stableDebtTokenAddress);
        const variableTokenContract = new web3.eth.Contract(abi.erc20, assetData.variableDebtTokenAddress);

        const aTokenName = await aTokenContract.methods.name().call();
        const aTokenSupply = await aTokenContract.methods.totalSupply().call();
        //const aTokenDecimals = await aTokenContract.methods.decimals().call();
        const aTokenSymbol = await aTokenContract.methods.symbol().call();

        const stableDebt = await stableTokenContract.methods.totalSupply().call();
        //const stableTokenDecimals = await stableTokenContract.methods.decimals().call();
        const stableTokenName = await stableTokenContract.methods.name().call()
        const stableTokenSymbol = await stableTokenContract.methods.symbol().call();

        const variableDebt = await variableTokenContract.methods.totalSupply().call();
        //const variableDecimals = await variableTokenContract.methods.decimals().call();
        const variableTokenName = await stableTokenContract.methods.name().call()
        const variableTokenSymbol = await variableTokenContract.methods.symbol().call();

        var stableDebtAdjusted = stableDebt / (10 ** decimals)
        var variableDebtAdjusted = variableDebt / (10 ** decimals)
        var aTokenSupplyAdjusted = aTokenSupply / (10 ** decimals)
        await tokenRef.update({
            [assetSymbol.replace(/[^A-Z0-9]+/ig, "").toUpperCase()]: {
                address: address,
                symbol: assetSymbol,
                name: assetName,
                price: price,
                supply: aTokenSupplyAdjusted,
                variableDebt: variableDebtAdjusted,
                stableDebt: stableDebtAdjusted,
                totalDebt: stableDebtAdjusted + variableDebtAdjusted,
                aTokenAddress: assetData.aTokenAddress,
                aTokenName: aTokenName,
                aTokenSymbol: aTokenSymbol,
                stableDebtTokenAddress: assetData.stableDebtTokenAddress,
                stableTokenName: stableTokenName,
                stableTokenSymbol: stableTokenSymbol,
                variableDebtTokenAddress: assetData.variableDebtTokenAddress,
                variableTokenName: variableTokenName,
                variableTokenSymbol: variableTokenSymbol,
                loanToValue: loanToValue,
                liquidationThreshold: liquidationThreshold,
                liquidationBonus: liquidationBonus,
                decimals: decimals,
                reserveIsActive: reserveIsActive,
                reserveIsFrozen: reserveIsFrozen,
                borrowingIsEnabled: borrowingIsEnabled,
                stableRateBorrowingEnabled: stableRateBorrowingEnabled,
                reserved: reserved,
                reserveFactor: reserveFactor,
                time: (new Date()).toUTCString()
            }
        })

    }
}



exports.helloPubSub = (event, context) => {
    return new Promise(function (resolve, reject) {
        getPoolData('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9').then(() => {
            console.log("done")
            resolve();
        })
    })

}; 