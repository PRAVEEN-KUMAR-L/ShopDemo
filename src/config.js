"use strict";

const PORT = 3000;
const UNOMI_BASE = "http://localhost:8181";
const UNOMI_AUTH = "Basic a2FyYWY6a2FyYWY=";  // karaf:karaf
const SCOPE = "fooddemo";

const DEMO_USERS = [
  { email: "alice@demo.com", name: "Alice Johnson", userId: "user-alice-001" },
  { email: "bob@demo.com", name: "Bob Smith", userId: "user-bob-002" },
  { email: "carol@demo.com", name: "Carol White", userId: "user-carol-003" },
];

module.exports = { PORT, UNOMI_BASE, UNOMI_AUTH, SCOPE, DEMO_USERS };
