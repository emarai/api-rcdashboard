const { socialKeys } = require("./near-social");
const retry = require("async-retry");
const { kv } = require("@vercel/kv");

export const statsTypeEnum = {
  mau: "mau",
  dau: "dau",
  githubActivities: "github_activities",
  totalLikes: "total_likes",
  totalWalletsCreated: "total_wallets_created",
  nftMints: "nft_mints",
  dappUsage: "dapp_usage",
  dappTimeline: "dapp_timeline",
  dappVolume: "dapp_volume",
};

export const doQueryToFlipside = async (query) => {
  // create run (https://docs.flipsidecrypto.com/flipside-api/rest-api)
  const headers = {};
  headers["Content-Type"] = "application/json";
  headers["x-api-key"] = process.env.FLIPSIDE_API_KEY;

  const requestResult = await fetch(`${process.env.FLIPSIDE_URL}`, {
    method: "POST",
    headers: headers,
    body: JSON.stringify({
      jsonrpc: "2.0",
      method: "createQueryRun",
      params: [
        {
          resultTTLHours: 24,
          maxAgeMinutes: 1440,
          sql: query,
          tags: {
            source: "postman-demo",
            env: "test",
          },
          dataSource: "snowflake-default",
          dataProvider: "flipside",
        },
      ],
      id: 1,
    }),
    redirect: "follow",
  });
  const requestJsonResult = await requestResult.json();

  const queryResultId = requestJsonResult.result.queryRun.id;

  let finalResult;
  await retry(
    async () => {
      const result = await fetch(`${process.env.FLIPSIDE_URL}`, {
        method: "POST",
        headers: headers,
        body: JSON.stringify({
          jsonrpc: "2.0",
          method: "getQueryRunResults",
          params: [
            {
              queryRunId: queryResultId,
              format: "json",
              page: {
                number: 1,
                size: 100,
              },
            },
          ],
          id: 1,
        }),
        redirect: "follow",
      });
      const jsonResult = await result.json();
      if (jsonResult.result.originalQueryRun.state != "QUERY_STATE_SUCCESS") {
        throw new Error("Not finished");
      } else {
        finalResult = jsonResult.result.rows;
      }
    },
    { retries: 10 }
  );
  return finalResult;
};

export const getMembers = async (accountId) => {
  if (!accountId) return [];

  const key = `${accountId}-members`;
  const cached = await kv.get(key);
  if (cached) return cached;

  let followers = new Set(
    Object.keys(
      (await socialKeys(`*/graph/follow/${accountId}`, null, {
        return_type: "BlockHeight",
        values_only: true,
      })) || {}
    )
  );

  let following = Object.keys(
    (
      await socialKeys(`${accountId}/graph/follow/*`, null, {
        return_type: "BlockHeight",
        values_only: true,
      })
    )?.[accountId]?.graph?.follow || {}
  );

  let members = [...new Set(following.filter((item) => followers.has(item)))];

  // The RC account is part of members
  members.push(accountId);

  await kv.set(key, members);
  await kv.expire(
    key,
    parseInt(process.env.CACHE_EXPIRE_MEMBERS_SEC) || 60 * 60 * 24 * 1
  );

  return members;
};

export const getRecursiveMembers = async (accountId) => {
  const key = `${accountId}-members-recursive`;
  const cached = await kv.get(key);
  if (cached) return cached;

  const members = await getMembers(accountId);
  const result = await Promise.all(
    members.map(async (rcAccountId) => await getMembers(rcAccountId))
  );

  const resultFinal = result.flat();

  await kv.set(key, resultFinal);
  await kv.expire(
    key,
    parseInt(process.env.CACHE_EXPIRE_MEMBERS_SEC) || 60 * 60 * 24 * 1
  );

  return resultFinal;
};

export const generateTotalLikes = async (members) => {
  if (members.length === 0) return [];
  const formattedMembers = JSON.stringify(members)
    .replaceAll("[", "(")
    .replaceAll("]", ")")
    .replaceAll('"', "'");
  const query = `SELECT
      COUNT(*) as total
        FROM
          near.social.fact_decoded_actions
        WHERE
          node = 'index'
          and JSON_EXTRACT_PATH_TEXT(node_data, 'like') != ''
          and signer_id in ${formattedMembers}
    `;

  return await doQueryToFlipside(query);
};

export const generateMAU = async (members) => {
  if (members.length === 0) return [];
  const formattedMembers = JSON.stringify(members)
    .replaceAll("[", "(")
    .replaceAll("]", ")")
    .replaceAll('"', "'");

  const query = `
    SELECT
        date_trunc('month', a.block_timestamp) AS "date",
        concat(
            date_part(year, "date"),
            '-',
            date_part(month, "date")
        ) as year_month,
        count(DISTINCT a.tx_signer) AS mau
    FROM
        near.core.fact_transactions a
    WHERE
        a.tx_signer != a.tx_receiver
    AND a.tx_signer IN ${formattedMembers}
    AND "date" > dateadd('month', -12, current_date)
    GROUP BY
        1
    ORDER BY
        1 DESC
    `;

  return await doQueryToFlipside(query);
};

export const generateDAU = async (members) => {
  if (members.length === 0) return [];
  const formattedMembers = JSON.stringify(members)
    .replaceAll("[", "(")
    .replaceAll("]", ")")
    .replaceAll('"', "'");

  const query = `
    SELECT
      date_trunc('day', a.block_timestamp) AS "date",
      concat(
        date_part(year, "date"),
        '-',
        date_part(month, "date"),
        '-',
        date_part(day, "date")
      ) as year_month,
      count(DISTINCT a.tx_signer) AS dau
    FROM
      near.core.fact_transactions a
    WHERE
      a.tx_signer != a.tx_receiver
      AND a.tx_signer IN ${formattedMembers}
      AND "date" > dateadd('month', -1, current_date)
    GROUP BY
      1
    ORDER BY
      1 desc
    `;

  return await doQueryToFlipside(query);
};

export const generateGithubActivities = async (members) => {
  if (members.length === 0) return [];
  const formattedMembers = JSON.stringify(members)
    .replaceAll("[", "(")
    .replaceAll("]", ")")
    .replaceAll('"', "'");

  const query = `
    WITH github_accounts AS (
      SELECT signer_id AS account,
      JSON_EXTRACT_PATH_TEXT(profile_data, 'github') AS github_account
      FROM
        near.social.fact_profile_changes
      WHERE
        profile_section = 'linktree'
        AND github_account != ''
        AND account IN ${formattedMembers}
    )
    SELECT
      date_trunc('month', ga.createdat) AS "date",
      concat(
        date_part(year, "date"),
        '-',
        date_part(month, "date")
      ) AS YEAR_MONTH,
      count(*) AS total_issues_and_pr
    FROM
      github_accounts a
      JOIN near.beta.github_activity ga ON a.github_account = ga.author
    GROUP BY
      1
    ORDER BY
      1 DESC;
    `;

  return await doQueryToFlipside(query);
};

export const generateTotalWalletsCreated = async (members) => {
  if (members.length === 0) return [];
  const formattedMembers = JSON.stringify(members)
    .replaceAll("[", "(")
    .replaceAll("]", ")")
    .replaceAll('"', "'");
  const query = `select
      count(*) as total
    from
      near.core.fact_receipts
    where
      receiver_id = 'near'
      AND actions:predecessor_id IN ${formattedMembers}
      AND actions:receipt:Action:actions[0]:FunctionCall:method_name = 'create_account'
    ;
  `;

  return await doQueryToFlipside(query);
};

export const generateNFTMints = async (members) => {
  if (members.length === 0) return [];
  const formattedMembers = JSON.stringify(members)
    .replaceAll("[", "(")
    .replaceAll("]", ")")
    .replaceAll('"', "'");
  const query = `SELECT
        date_trunc('month', block_timestamp) AS "date",
        concat(
          date_part(year, "date"),
          '-',
          date_part(month, "date")
        ) AS YEAR_MONTH,
        COUNT(DISTINCT tx_hash) as total_activity
    FROM
      near.nft.fact_nft_mints
    WHERE (receiver_id IN ${formattedMembers} OR owner_id IN ${formattedMembers})
    GROUP BY 1;
  `;

  return await doQueryToFlipside(query);
};

export const generateDappUsage = async (members) => {
  if (members.length === 0) return [];
  const formattedMembers = JSON.stringify(members)
    .replaceAll("[", "(")
    .replaceAll("]", ")")
    .replaceAll('"', "'");
  const query = `with lst_top_dApps as (
      select top 20
        INITCAP( PROJECT_NAME) as dApp
        ,count(DISTINCT block_timestamp::date) as "Activity days"
        ,count(DISTINCT tx_hash) as TXs
        ,TXs / "Activity days" as "Transaction per day"
      from near.core.fact_transactions
        join near.core.dim_address_labels on address = TX_RECEIVER
      where label_type='dapp' and tx_signer in ${formattedMembers}
      group by 1
      )
      select * from lst_top_dApps;
  `;

  return await doQueryToFlipside(query);
};

export const generateDappVolume = async (members) => {
  if (members.length === 0) return [];
  const formattedMembers = JSON.stringify(members)
    .replaceAll("[", "(")
    .replaceAll("]", ")")
    .replaceAll('"', "'");
  const query = `with lst_dapps as (
    select top 20
        concat(PROJECT_NAME, '-', tx_receiver) as dApp
        ,count(DISTINCT tx_hash) as TXs
        ,count(DISTINCT tx_signer) as wallets
        ,sum(deposit/pow(10,24)) as volume_NEAR
    from near.core.fact_transfers
      join near.core.dim_address_labels on address = TX_RECEIVER
    where label_type='dapp'
      and tx_signer in ${formattedMembers}
      and status=true
    group by 1
    order by volume_NEAR desc
  )
  select * from lst_dapps
  `;

  return await doQueryToFlipside(query);
};

export const generateDappTimeline = async (members) => {
  if (members.length === 0) return [];
  const formattedMembers = JSON.stringify(members)
    .replaceAll("[", "(")
    .replaceAll("]", ")")
    .replaceAll('"', "'");
  const query = `
  with lst_top_dApps as (
    select top 20
      INITCAP( PROJECT_NAME) as dApp
      ,count(DISTINCT block_timestamp::date) as "Activity days"
      ,count(DISTINCT tx_hash) as TXs
      ,TXs / "Activity days" as "Transaction per day"
    from near.core.fact_transactions
      join near.core.dim_address_labels on address = TX_RECEIVER
    where label_type='dapp'
    group by 1
    order by TXs desc
    )
    select   
      date_trunc(week,block_timestamp)::date as date
      ,INITCAP( PROJECT_NAME) as dApp
      ,count(DISTINCT tx_hash) as TXs
    from near.core.fact_transactions
      join near.core.dim_address_labels on address = TX_RECEIVER
    where label_type='dapp'
      and dApp in(select dApp from lst_top_dApps)
      and block_timestamp::date > dateadd('month', -12, current_date)
      and tx_signer in ${formattedMembers}
    group by 1,2
    order by 1
  `;

  return await doQueryToFlipside(query);
};
