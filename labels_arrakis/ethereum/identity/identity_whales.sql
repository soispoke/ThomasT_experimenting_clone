CREATE OR REPLACE VIEW identity_whales AS

SELECT
 DISTINCT
 from as address,
 'Ethereum' AS blockchain,
 'Identity' AS category,
 CASE 
 WHEN from = '0xe1d29d0a39962a9a8d2a297ebe82e166f8b8ec18' THEN '@KeyboardMonkey3'
 WHEN from = '0xb1adceddb2941033a090dd166a462fe1c2029484' THEN '0xb1'
 WHEN from = '0x5028d77b91a3754fb38b2fbb726af02d1fe44db6' THEN '0xb1'
 WHEN from = '0xfd22004806a6846ea67ad883356be810f0428793' THEN '6529 Museum: Transaction Wallet'
 WHEN from = '0x431e81e5dfb5a24541b5ff8762bdef3f32f96354' THEN 'Andre Cronje'
 WHEN from = '0xc6b0562605D35eE710138402B878ffe6F2E23807' THEN 'Beeple'
 WHEN from = '0x8a7f7c5b556b1298a74c0e89df46eba117a2f6c1' THEN 'Daniele Sesta'
 WHEN from = '0xd6a984153acb6c9e2d788f08c2465a1358bb89a7' THEN 'Gary Vee'
 WHEN from = '0x3ddfa8ec3052539b6c9549f12cea2c295cff5296' THEN 'Justin Sun'
 WHEN from = '0xa679c6154b8d4619af9f83f0bf9a13a680e01ecf' THEN 'Mark Cuban'
 WHEN from = '0x293ed38530005620e4b28600f196a97e1125daac' THEN 'Mark Cuban'
 WHEN from = '0xcba1a275e2d858ecffaf7a87f606f74b719a8a93' THEN 'MJthePopstar'
 WHEN from = '0xd387a6e4e84a6c86bd90c158c6028a58cc8ac459' THEN 'Pransky'
 WHEN from = '0xc79b1cb9e38af3a2dee4b46f84f87ae5c36c679c' THEN 'Pransky'
 WHEN from = '0xc5ed2333f8a2c351fca35e5ebadb2a82f5d254c3' THEN 'SBF'
 WHEN from = '0x84d34f4f83a87596cd3fb6887cff8f17bf5a7b83' THEN 'SBF'
 WHEN from = '0x0f4ee9631f4be0a63756515141281a3e2b293bbe' THEN 'SBF'
 WHEN from = '0x220866b1a2219f40e72f5c628b65d54268ca3a9d' THEN 'Vitalik Buterin'
 WHEN from = '0xab5801a7d398351b8be11c439e05c5b3259aec9b' THEN 'Vitalik Buterin'
 WHEN from = '0x1db3439a222c519ab44bb1144fc28167b4fa6ee6' THEN 'Vitalik Buterin'
 WHEN from = '0x020ca66c30bec2c4fe3861a94e4db4a498a35872' THEN 'YFI Whale'
 WHEN from = '0x085af684acdb1220d111fee971b733c5e5ae6ccd' THEN 'Zhu Su'
 WHEN from = '0x4862733b5fddfd35f35ea8ccf08f5045e57388b3' THEN 'Zhu Su' END as name,
cast(NULL AS STRING) AS metric,
cast(NULL AS ARRAY<STRING>) AS details,
cast(NULL AS BIGINT) AS quantity,
cast(NULL AS DECIMAL) AS percentile,
cast(NULL AS BIGINT) AS rank,
'query' AS source,
'soispoke' AS author
FROM jonas10_ethereum.transactions
WHERE from IN ('0xe1d29d0a39962a9a8d2a297ebe82e166f8b8ec18',
                 '0xb1adceddb2941033a090dd166a462fe1c2029484',
                 '0x5028d77b91a3754fb38b2fbb726af02d1fe44db6',
                 '0xfd22004806a6846ea67ad883356be810f0428793',
                 '0x431e81e5dfb5a24541b5ff8762bdef3f32f96354',
                 '0xc6b0562605D35eE710138402B878ffe6F2E23807',
                 '0x8a7f7c5b556b1298a74c0e89df46eba117a2f6c1',
                 '0xd6a984153acb6c9e2d788f08c2465a1358bb89a7',
                 '0x3ddfa8ec3052539b6c9549f12cea2c295cff5296',
                 '0xa679c6154b8d4619af9f83f0bf9a13a680e01ecf',
                 '0x293ed38530005620e4b28600f196a97e1125daac',
                 '0xcba1a275e2d858ecffaf7a87f606f74b719a8a93',
                 '0xd387a6e4e84a6c86bd90c158c6028a58cc8ac459',
                 '0xc79b1cb9e38af3a2dee4b46f84f87ae5c36c679c',
                 '0xc5ed2333f8a2c351fca35e5ebadb2a82f5d254c3',
                 '0x84d34f4f83a87596cd3fb6887cff8f17bf5a7b83',
                 '0x0f4ee9631f4be0a63756515141281a3e2b293bbe',
                 '0x220866b1a2219f40e72f5c628b65d54268ca3a9d',
                 '0xab5801a7d398351b8be11c439e05c5b3259aec9b',
                 '0x1db3439a222c519ab44bb1144fc28167b4fa6ee6',
                 '0x020ca66c30bec2c4fe3861a94e4db4a498a35872',
                 '0x085af684acdb1220d111fee971b733c5e5ae6ccd',
                 '0x4862733b5fddfd35f35ea8ccf08f5045e57388b3');