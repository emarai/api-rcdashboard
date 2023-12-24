const app = require("express")();
const { v4 } = require("uuid");
const { socialKeys } = require("./near-social");
const {
  statsTypeEnum,
  generateTotalLikes,
  getMembers,
  getRecursiveMembers,
} = require("./stats");

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

  let members;
  if (account_id === "rc-dao.near") {
    members = await getRecursiveMembers(account_id);
  } else {
    members = await getMembers(account_id);
  }

  let result;
  if (stats_type === statsTypeEnum.totalLikes) {
    result = await generateTotalLikes(members);
  }

  res.json(result);
});

module.exports = app;
