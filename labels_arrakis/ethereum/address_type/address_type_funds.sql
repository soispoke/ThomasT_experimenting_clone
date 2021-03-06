 CREATE OR REPLACE VIEW address_type_funds AS

 SELECT
 DISTINCT
 from as address,
 'Ethereum' AS blockchain,
 'Address Type' AS category,
 CASE 
 WHEN from IN ('0x2B1Ad6184a6B0fac06bD225ed37C2AbC04415fF4',
                 '0x05e793ce0c6027323ac150f6d45c2344d28b6019',
                 '0xa294cca691e4c83b1fc0c8d63d9a3eef0a196de1',
                 '0xe05a884d4653289916d54ce6ae0967707c519879',
                 '0x0f4ee9631f4be0a63756515141281a3e2b293bbe',
                 '0xc5ed2333f8a2c351fca35e5ebadb2a82f5d2ÒÒ54c3',
                 '0x84d34f4f83a87596cd3fb6887cff8f17bf5a7b83',
                 '0x4c8cfe078a5b989cea4b330197246ced82764c63',
                 '0x712d0f306956a6a4b4f9319ad9b9de48c5345996',
                 '0xafa64cca337efee0ad827f6c2684e69275226e90',
                 '0x9b5ea8c719e29a5bd0959faf79c9e5c8206d0499',
                 '0xf584f8728b874a6a5c7a8d4d387c9aae9172d621',
                 '0x112b69178d416cd03222de9e6dd6b3adf36412aa',
                 '0xc8d328b21f476a4b6e0681f6e4e41693a220347d',
                 '0x66b870ddf78c975af5cd8edc6de25eca81791de1',
                 '0xd9b012a168fb6c1b71c24db8cee1a256b3caa2a2',
                 '0x4655b7ad0b5f5bacb9cf960bbffceb3f0e51f363',
                 '0x287f0e1826d88d0d212ff4a0ede781186da575a2',
                 '0x80c2c1ceb335e39b7021c646fd3ec159faf9109d',
                 '0x0548f59fee79f8832c299e01dca5c76f034f558e',
                 '0x4862733b5fddfd35f35ea8ccf08f5045e57388b3',
                 '0x085af684acdb1220d111fee971b733c5e5ae6ccd',
                 '0x8e04af7f7c76daa9ab429b1340e0327b5b835748',
                 '0x0000006daea1723962647b7e189d311d757fb793') THEN 'Fund' END as name,
cast(NULL AS STRING) AS metric,
cast(NULL AS ARRAY<STRING>) AS details,
cast(NULL AS BIGINT) AS quantity,
cast(NULL AS DECIMAL) AS percentile,
cast(NULL AS BIGINT) AS rank,
'query' AS source,
'soispoke' AS author
FROM ethereum.transactions
WHERE from IN ('0x2B1Ad6184a6B0fac06bD225ed37C2AbC04415fF4',
                 '0x05e793ce0c6027323ac150f6d45c2344d28b6019',
                 '0xa294cca691e4c83b1fc0c8d63d9a3eef0a196de1',
                 '0xe05a884d4653289916d54ce6ae0967707c519879',
                 '0x0f4ee9631f4be0a63756515141281a3e2b293bbe',
                 '0xc5ed2333f8a2c351fca35e5ebadb2a82f5d254c3',
                 '0x84d34f4f83a87596cd3fb6887cff8f17bf5a7b83',
                 '0x4c8cfe078a5b989cea4b330197246ced82764c63',
                 '0x712d0f306956a6a4b4f9319ad9b9de48c5345996',
                 '0xafa64cca337efee0ad827f6c2684e69275226e90',
                 '0x9b5ea8c719e29a5bd0959faf79c9e5c8206d0499',
                 '0xf584f8728b874a6a5c7a8d4d387c9aae9172d621',
                 '0x112b69178d416cd03222de9e6dd6b3adf36412aa',
                 '0xc8d328b21f476a4b6e0681f6e4e41693a220347d',
                 '0x66b870ddf78c975af5cd8edc6de25eca81791de1',
                 '0xd9b012a168fb6c1b71c24db8cee1a256b3caa2a2',
                 '0x4655b7ad0b5f5bacb9cf960bbffceb3f0e51f363',
                 '0x287f0e1826d88d0d212ff4a0ede781186da575a2',
                 '0x80c2c1ceb335e39b7021c646fd3ec159faf9109d',
                 '0x0548f59fee79f8832c299e01dca5c76f034f558e',
                 '0x4862733b5fddfd35f35ea8ccf08f5045e57388b3',
                 '0x085af684acdb1220d111fee971b733c5e5ae6ccd',
                 '0x8e04af7f7c76daa9ab429b1340e0327b5b835748',
                 '0x0000006daea1723962647b7e189d311d757fb793');