CREATE OR REPLACE VIEW identity_exchanges AS

SELECT DISTINCT
 from as address,
 'Ethereum' AS blockchain,
 'Identity' AS category,
 CASE 
 WHEN from = '0xfe9e8709d3215310075d67e3ed32a380ccf451c8' THEN 'Binance'
 WHEN from = '0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67' THEN 'Binance'
 WHEN from = '0xbe0eb53f46cd790cd13851d5eff43d12404d33e8' THEN 'Binance'
 WHEN from = '0xf977814e90da44bfa03b6295a0616a897441acec' THEN 'Binance'
 WHEN from = '0x001866ae5b3de6caa5a51543fd9fb64f524f5478' THEN 'Binance'
 WHEN from = '0x85b931a32a0725be14285b66f1a22178c672d69b' THEN 'Binance'
 WHEN from = '0x708396f17127c42383e3b9014072679b2f60b82f' THEN 'Binance'
 WHEN from = '0xe0f0cfde7ee664943906f17f7f14342e76a5cec7' THEN 'Binance'
 WHEN from = '0x8f22f2063d253846b53609231ed80fa571bc0c8f' THEN 'Binance'
 WHEN from = '0x0681d8db095565fe8a346fa0277bffde9c0edbbf' THEN 'Binance'
 WHEN from = '0x564286362092d8e7936f0549571a803b203aaced' THEN 'Binance'
 WHEN from = '0xd551234ae421e3bcba99a0da6d736074f22192ff' THEN 'Binance'
 WHEN from = '0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be' THEN 'Binance'
 WHEN from = '0x28c6c06298d514db089934071355e5743bf21d60' THEN 'Binance 14'
 WHEN from = '0x21a31Ee1afC51d94C2eFcCAa2092aD1028285549' THEN 'Binance 15'
 WHEN from = '0xDFd5293D8e347dFe59E90eFd55b2956a1343963d' THEN 'Binance 16' 
 WHEN from = '0x56Eddb7aa87536c09CCc2793473599fD21A8b17F' THEN 'Binance 17' 
 WHEN from = '0x9696f59E4d72E237BE84fFD425DCaD154Bf96976' THEN 'Binance 18'
 WHEN from = '0x4D9fF50EF4dA947364BB9650892B2554e7BE5E2B' THEN 'Binance 19'
 WHEN from = '0x4976A4A02f38326660D17bf34b431dC6e2eb2327' THEN 'Binance 20'
 WHEN from = '0x1151314c646ce4e0efd76d1af4760ae66a9fe30f' THEN 'Bitfinex'
 WHEN from = '0x742d35cc6634c0532925a3b844bc454e4438f44e' THEN 'Bitfinex'
 WHEN from = '0xab7c74abc0c4d48d1bdad5dcb26153fc8780f83e' THEN 'Bitfinex'
 WHEN from = '0x876eabf441b2ee5b5b0554fd502a8e0600950cfa' THEN 'Bitfinex 3'
 WHEN from = '0xe79eef9b9388a4ff70ed7ec5bccd5b928ebb8bd1' THEN 'Bitmart'
 WHEN from = '0x68b22215ff74e3606bd5e6c1de8c2d68180c85f7' THEN 'Bitmart'
 WHEN from = '0x03bdf69b1322d623836afbd27679a1c0afa067e9' THEN 'Bitmart'
 WHEN from = '0x4b1a99467a284cc690e3237bc69105956816f762' THEN 'Bitmart'
 WHEN from = '0x986a2fca9eda0e06fbf7839b89bfc006ee2a23dd' THEN 'Bitmart'
 WHEN from = '0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98' THEN 'Bittrex' 
 WHEN from = '0xe94b04a0fed112f3664e45adb2b8915693dd5ff3' THEN 'Bittrex' 
 WHEN from = '0x66f820a414680b5bcda5eeca5dea238543f42054' THEN 'Bittrex' 
 WHEN from = '0x00bdb5699745f5b860228c8f939abf1b9ae374ed' THEN 'Bitstamp 1'
 WHEN from = '0x808b4da0be6c9512e948521452227efc619bea52' THEN 'BlockFi 5' 
 WHEN from = '0x71660c4005ba85c37ccec55d0c4493e66fe775d3' THEN 'Coinbase' 
 WHEN from = '0xb5d85cbf7cb3ee0d56b3bb207d5fc4b82f43f511' THEN 'Coinbase' 
 WHEN from = '0x6262998ced04146fa42253a5c0af90ca02dfd2a3' THEN 'Crypto.com'
 WHEN from = '0x46340b20830761efd32832a74d7169b29feb9758' THEN 'Crypto.com'
 WHEN from = '0x742d35cc6634c0532925a3b844bc454e4438f44e' THEN 'Crypto.com'
 WHEN from = '0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2' THEN 'FTX'
 WHEN from = '0xc098b2a3aa256d2140208c3de6543aaef5cd3a94' THEN 'FTX'
 WHEN from = '0x0d0707963952f2fba59dd06f2b425ace40b492fe' THEN 'Gate.io' 
 WHEN from = '0x7793cd85c11a924478d358d49b05b37e91b5810f' THEN 'Gate.io' 
 WHEN from = '0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c' THEN 'Gate.io'
 WHEN from = '0xd793281182a0e3e023116004778f45c29fc14f19' THEN 'Gate.io'
 WHEN from = '0xd24400ae8bfebb18ca49be86258a3c749cf46853' THEN 'Gemini'
 WHEN from = '0x5f65f7b609678448494de4c87521cdf6cef1e932' THEN 'Gemini'
 WHEN from = '0x07ee55aa48bb72dcc6e9d78256648910de513eca' THEN 'Gemini'
 WHEN from = '0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8' THEN 'Gemini'
 WHEN from = '0x61edcdf5bb737adffe5043706e7c5bb1f1a56eea' THEN 'Gemini'
 WHEN from = '0xe93381fb4c4f14bda253907b18fad305d799241a' THEN 'Huobi' 
 WHEN from = '0xab5c66752a9e8167967685f1450532fb96d5d24f' THEN 'Huobi 1' 
 WHEN from = '0x6748f50f686bfbca6fe8ad62b22228b87f31ff2b' THEN 'Huobi 2' 
 WHEN from = '0x2910543af39aba0cd09dbb2d50200b3e800a63d2' THEN 'Kraken'
 WHEN from = '0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13' THEN 'Kraken'
 WHEN from = '0xe853c56864a2ebe4576a807d26fdc4a0ada51919' THEN 'Kraken'
 WHEN from = '0xfa52274dd61e1643d2205169732f29114bc240b3' THEN 'Kraken'
 WHEN from = '0x89e51fa8ca5d66cd220baed62ed01e8951aa7c40' THEN 'Kraken'
 WHEN from = '0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0' THEN 'Kraken 4'
 WHEN from = '0xec30d02f10353f8efc9601371f56e808751f396f' THEN 'KuCoin' 
 WHEN from = '0x2b5634c42055806a59e9107ed44d43c426e58258' THEN 'KuCoin' 
 WHEN from = '0x689c56aef474df92d44a1b70850f808488f9769c' THEN 'KuCoin' 
 WHEN from = '0x4ad64983349c49defe8d7a4686202d24b25d0ce8' THEN 'KuCoin' 
 WHEN from = '0xa1d8d972560c2f8144af871db508f0b0b10a3fbf' THEN 'KuCoin 3' 
 WHEN from = '0x6cc5f688a315f3dc28a7781717a9a798a59fda7b' THEN 'OkEx'
 WHEN from = '0x236f9f97e0e62388479bf9e5ba4889e46b0273c3' THEN 'OkEx'
 WHEN from = '0xa7efae728d2936e78bda97dc267687568dd593f3' THEN 'OkEx'
 WHEN from = '0x32be343b94f860124dc4fee278fdcbd38c102d88' THEN 'Poloniex' 
 WHEN from = '0xa910f92acdaf488fa6ef02174fb86208ad7722ba' THEN 'Poloniex 4' END as name,
cast(NULL AS STRING) AS metric,
cast(NULL AS ARRAY<STRING>) AS details,
cast(NULL AS BIGINT) AS quantity,
cast(NULL AS DECIMAL) AS percentile,
cast(NULL AS BIGINT) AS rank,
'query' AS source,
'soispoke' AS author
FROM ethereum.transactions
WHERE from IN   ('0xfe9e8709d3215310075d67e3ed32a380ccf451c8',
                 '0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67',
                 '0xbe0eb53f46cd790cd13851d5eff43d12404d33e8',
                 '0xf977814e90da44bfa03b6295a0616a897441acec',
                 '0x001866ae5b3de6caa5a51543fd9fb64f524f5478',
                 '0x85b931a32a0725be14285b66f1a22178c672d69b',
                 '0x708396f17127c42383e3b9014072679b2f60b82f',
                 '0xe0f0cfde7ee664943906f17f7f14342e76a5cec7',
                 '0x8f22f2063d253846b53609231ed80fa571bc0c8f',
                 '0x0681d8db095565fe8a346fa0277bffde9c0edbbf',
                 '0x564286362092d8e7936f0549571a803b203aaced',
                 '0xd551234ae421e3bcba99a0da6d736074f22192ff',
                 '0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be',
                 '0x28c6c06298d514db089934071355e5743bf21d60',
                 '0x21a31Ee1afC51d94C2eFcCAa2092aD1028285549',
                 '0xDFd5293D8e347dFe59E90eFd55b2956a1343963d',
                 '0x56Eddb7aa87536c09CCc2793473599fD21A8b17F',
                 '0x9696f59E4d72E237BE84fFD425DCaD154Bf96976',
                 '0x4D9fF50EF4dA947364BB9650892B2554e7BE5E2B',
                 '0x4976A4A02f38326660D17bf34b431dC6e2eb2327',
                 '0x1151314c646ce4e0efd76d1af4760ae66a9fe30f',
                 '0x742d35cc6634c0532925a3b844bc454e4438f44e',
                 '0xab7c74abc0c4d48d1bdad5dcb26153fc8780f83e',
                 '0x876eabf441b2ee5b5b0554fd502a8e0600950cfa',
                 '0xe79eef9b9388a4ff70ed7ec5bccd5b928ebb8bd1',
                 '0x68b22215ff74e3606bd5e6c1de8c2d68180c85f7',
                 '0x03bdf69b1322d623836afbd27679a1c0afa067e9',
                 '0x4b1a99467a284cc690e3237bc69105956816f762',
                 '0x986a2fca9eda0e06fbf7839b89bfc006ee2a23dd',
                 '0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98',
                 '0xe94b04a0fed112f3664e45adb2b8915693dd5ff3',
                 '0x66f820a414680b5bcda5eeca5dea238543f42054',
                 '0x00bdb5699745f5b860228c8f939abf1b9ae374ed',
                 '0x808b4da0be6c9512e948521452227efc619bea52',
                 '0x71660c4005ba85c37ccec55d0c4493e66fe775d3',
                 '0xb5d85cbf7cb3ee0d56b3bb207d5fc4b82f43f511',
                 '0x6262998ced04146fa42253a5c0af90ca02dfd2a3',
                 '0x46340b20830761efd32832a74d7169b29feb9758',
                 '0x742d35cc6634c0532925a3b844bc454e4438f44e',
                 '0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2',
                 '0xc098b2a3aa256d2140208c3de6543aaef5cd3a94',
                 '0x0d0707963952f2fba59dd06f2b425ace40b492fe',
                 '0x7793cd85c11a924478d358d49b05b37e91b5810f',
                 '0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c',
                 '0xd793281182a0e3e023116004778f45c29fc14f19',
                 '0xd24400ae8bfebb18ca49be86258a3c749cf46853',
                 '0x5f65f7b609678448494de4c87521cdf6cef1e932',
                 '0x07ee55aa48bb72dcc6e9d78256648910de513eca',
                 '0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8',
                 '0x61edcdf5bb737adffe5043706e7c5bb1f1a56eea',
                 '0xe93381fb4c4f14bda253907b18fad305d799241a',
                 '0xab5c66752a9e8167967685f1450532fb96d5d24f',
                 '0x6748f50f686bfbca6fe8ad62b22228b87f31ff2b',
                 '0x2910543af39aba0cd09dbb2d50200b3e800a63d2',
                 '0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13',
                 '0xe853c56864a2ebe4576a807d26fdc4a0ada51919',
                 '0xfa52274dd61e1643d2205169732f29114bc240b3',
                 '0x89e51fa8ca5d66cd220baed62ed01e8951aa7c40',
                 '0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0',
                 '0xec30d02f10353f8efc9601371f56e808751f396f',
                 '0x2b5634c42055806a59e9107ed44d43c426e58258',
                 '0x689c56aef474df92d44a1b70850f808488f9769c',
                 '0x4ad64983349c49defe8d7a4686202d24b25d0ce8',
                 '0xa1d8d972560c2f8144af871db508f0b0b10a3fbf',
                 '0x6cc5f688a315f3dc28a7781717a9a798a59fda7b',
                 '0x236f9f97e0e62388479bf9e5ba4889e46b0273c3',
                 '0xa7efae728d2936e78bda97dc267687568dd593f3',
                 '0x32be343b94f860124dc4fee278fdcbd38c102d88',
                 '0xa910f92acdaf488fa6ef02174fb86208ad7722ba');