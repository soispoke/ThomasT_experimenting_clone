SELECT nft.insert_looksrare_v2_beta(
    (SELECT now() - interval '1 day'),
    now(),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '1 day'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
);

SELECT nft.insert_cryptopunks_v2_beta(
    (SELECT now() - interval '1 day'),
    now(),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '1 day'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
);

SELECT nft.insert_foundation_v2_beta(
    (SELECT now() - interval '1 day'),
    now(),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '1 day'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
);

SELECT nft.insert_superrare_v2_beta(
    (SELECT now() - interval '1 day'),
    now(),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '1 day'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
);

SELECT nft.insert_rarible_v2_beta(
    (SELECT now() - interval '1 day'),
    now(),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '1 day'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
);


BEGIN;
SELECT nft.insert_wyvern_data(
    (SELECT now() - interval '1 day'),
    now(),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '1 day'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
);


SELECT nft.insert_opensea_v2_beta(
    (SELECT now() - interval '1 day'),
    now(),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '1 day'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
);
COMMIT;

