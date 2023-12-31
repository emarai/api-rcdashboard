const app = require("express")();
const { v4 } = require("uuid");
const { socialKeys } = require("./near-social");
const {
  statsTypeEnum,
  generateTotalLikes,
  getMembers,
  getRecursiveMembers,
  generateDappUsage,
  generateDAU,
  generateMAU,
  generateNFTMints,
  generateGithubActivities,
  generateTotalWalletsCreated,
} = require("./stats");
const { kv } = require("@vercel/kv");

app.get("/api", (req, res) => {
  const path = `/api/item/${v4()}`;
  res.setHeader("Content-Type", "text/html");
  res.setHeader("Cache-Control", "s-max-age=1, stale-while-revalidate");
  res.end(`Hello! Go to item: <a href="${path}">${path}</a>`);
});

app.get("/api/item/:slug", (req, res) => {
  const { slug } = req.params;
  res.end(`Item: ${slug}`);
});

app.get("/api/run", async (req, res) => {
  const { account_id, stats_type } = req.query;

  const key = `${account_id}-${stats_type}`;
  const cached = await kv.get(key);
  if (cached) return res.json(cached);

  let members;
  if (account_id === "rc-dao.near") {
    members = await getRecursiveMembers(account_id);
  } else {
    members = await getMembers(account_id);
  }

  let result;
  if (stats_type === statsTypeEnum.totalLikes) {
    result = await generateTotalLikes(members);
  } else if (stats_type === statsTypeEnum.dappUsage) {
    result = await generateDappUsage(members);
  } else if (stats_type === statsTypeEnum.dau) {
    result = await generateDAU(members);
  } else if (stats_type === statsTypeEnum.mau) {
    result = await generateMAU(members);
  } else if (stats_type === statsTypeEnum.nftMints) {
    result = await generateNFTMints(members);
  } else if (stats_type === statsTypeEnum.githubActivities) {
    result = await generateGithubActivities(members);
  } else if (stats_type === statsTypeEnum.totalWalletsCreated) {
    result = await generateTotalWalletsCreated(members);
  }

  await kv.set(key, result);
  await kv.expire(
    key,
    parseInt(process.env.CACHE_EXPIRE_SEC) || 60 * 60 * 24 * 3
  );
  res.json(result);
});

module.exports = app;
