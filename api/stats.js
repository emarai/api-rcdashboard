const { socialKeys } = require("./near-social");
const retry = require("async-retry");

export const statsTypeEnum = {
  mau: "mau",
  dau: "dau",
  githubActivities: "github_activities",
  totalLikes: "total_likes",
  totalWalletsCreated: "total_wallets_created",
  nftMints: "nft_mints",
  dappUsage: "dapp_usage",
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
      if (!jsonResult.result.rows) {
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

  let followers = new Set(
    Object.keys(
      await socialKeys(`*/graph/follow/${accountId}`, null, {
        return_type: "BlockHeight",
        values_only: true,
      }) || {}
    )
  );

  let following = Object.keys(
    (await socialKeys(
      `${accountId}/graph/follow/*`,
      null,
      {
        return_type: "BlockHeight",
        values_only: true,
      }
    ))?.[accountId]?.graph?.follow || {}
  );

  let members = [...new Set(following.filter((item) => followers.has(item)))];

  // The RC account is part of members
  members.push(accountId);

  return members;
};

export const getRecursiveMembers = async (accountId) => {
  const members = await getMembers(accountId);
  const result = await Promise.all(members.map(
    async (rcAccountId) => await getMembers(rcAccountId)
  ));
  return result.flat();
};

export const generateTotalLikes = (members) => {
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

  return doQueryToFlipside(query);
};
