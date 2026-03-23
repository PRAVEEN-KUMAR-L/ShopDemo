"use strict";

const { unomiRequest } = require("./unomiHelper");
const { SCOPE } = require("./config");

// ─── Bootstrap ────────────────────────────────────────────────────────────────
async function bootstrapUnomi() {
  console.log("\n[BOOTSTRAP] Starting Unomi 2.6 configuration\n");

  // 1. Scope
  try {
    const r = await unomiRequest("POST", "/cxs/scopes", {
      itemId: SCOPE, itemType: "scope",
      metadata: { id: SCOPE, name: "Ecommerce Demo Scope" },
    });
    console.log("[BOOTSTRAP] Scope:", r.ok || r.status === 409 ? "OK" : r.status, r.ok ? "" : JSON.stringify(r.body));
  } catch (e) { console.error("[BOOTSTRAP] Scope:", e.message); }

  // 2. JSON Schemas
  const schemas = [
    {
      $schema: "https://json-schema.org/draft/2019-09/schema",
      $id: "https://fooddemo.com/schemas/events/addToCart/1-0-0",
      self: { vendor: "com.fooddemo", name: "addToCart", format: "jsonschema", target: "events", version: "1-0-0" },
      title: "addToCart", type: "object",
      allOf: [{ $ref: "https://unomi.apache.org/schemas/json/event/1-0-0" }],
      properties: {
        source: { $ref: "https://unomi.apache.org/schemas/json/item/1-0-0" },
        target: { $ref: "https://unomi.apache.org/schemas/json/item/1-0-0" },
        properties: {
          type: "object", properties: {
            productId: { type: ["string", "null"] },
            productName: { type: ["string", "null"] },
            category: { type: ["string", "null"] },
            price: { type: ["number", "null"] },
          }
        },
      },
      unevaluatedProperties: false,
    },
    {
      $schema: "https://json-schema.org/draft/2019-09/schema",
      $id: "https://fooddemo.com/schemas/events/removeFromCart/1-0-0",
      self: { vendor: "com.fooddemo", name: "removeFromCart", format: "jsonschema", target: "events", version: "1-0-0" },
      title: "removeFromCart", type: "object",
      allOf: [{ $ref: "https://unomi.apache.org/schemas/json/event/1-0-0" }],
      properties: {
        source: { $ref: "https://unomi.apache.org/schemas/json/item/1-0-0" },
        target: { $ref: "https://unomi.apache.org/schemas/json/item/1-0-0" },
        properties: {
          type: "object", properties: {
            productId: { type: ["string", "null"] },
          }
        },
      },
      unevaluatedProperties: false,
    },
    {
      $schema: "https://json-schema.org/draft/2019-09/schema",
      $id: "https://fooddemo.com/schemas/events/purchaseComplete/1-0-0",
      self: { vendor: "com.fooddemo", name: "purchaseComplete", format: "jsonschema", target: "events", version: "1-0-0" },
      title: "purchaseComplete", type: "object",
      allOf: [{ $ref: "https://unomi.apache.org/schemas/json/event/1-0-0" }],
      properties: {
        source: { $ref: "https://unomi.apache.org/schemas/json/item/1-0-0" },
        target: { $ref: "https://unomi.apache.org/schemas/json/item/1-0-0" },
        properties: {
          type: "object", properties: {
            orderId: { type: ["string", "null"] },
            totalAmount: { type: ["number", "null"] },
            itemCount: { type: ["number", "null"] },
          }
        },
      },
      unevaluatedProperties: false,
    },
    {
      $schema: "https://json-schema.org/draft/2019-09/schema",
      $id: "https://fooddemo.com/schemas/events/viewProduct/1-0-0",
      self: { vendor: "com.fooddemo", name: "viewProduct", format: "jsonschema", target: "events", version: "1-0-0" },
      title: "viewProduct", type: "object",
      allOf: [{ $ref: "https://unomi.apache.org/schemas/json/event/1-0-0" }],
      properties: {
        source: { $ref: "https://unomi.apache.org/schemas/json/item/1-0-0" },
        target: { $ref: "https://unomi.apache.org/schemas/json/item/1-0-0" },
        properties: {
          type: "object", properties: {
            productId: { type: ["string", "null"] },
            productName: { type: ["string", "null"] },
            category: { type: ["string", "null"] },
            price: { type: ["number", "null"] },
          }
        },
      },
      unevaluatedProperties: false,
    },
  ];

  for (const s of schemas) {
    try {
      const r = await unomiRequest("POST", "/cxs/jsonSchema", s);
      console.log(`[BOOTSTRAP] Schema '${s.title}':`, r.ok || r.status === 409 ? "OK" : r.status, r.ok ? "" : JSON.stringify(r.body));
    } catch (e) { console.error(`[BOOTSTRAP] Schema '${s.title}':`, e.message); }
  }

  // 3. Rules — delete then recreate to guarantee stale definitions are replaced
  const rules = [
    {
      metadata: { id: "trackNorthIndianInterest", name: "Track North Indian Interest", scope: SCOPE, enabled: true },
      condition: {
        type: "booleanCondition",
        parameterValues: {
          operator: "and", subConditions: [
            { type: "eventTypeCondition", parameterValues: { eventTypeId: "addToCart" } },
            { type: "eventPropertyCondition", parameterValues: { propertyName: "properties.category", comparisonOperator: "equals", propertyValue: "North Indian" } },
          ]
        },
      },
      actions: [{ type: "setPropertyAction", parameterValues: { setPropertyName: "properties.interestedInNorthIndian", setPropertyValue: "true", setPropertyStrategy: "alwaysSet" } }],
    },
    {
      metadata: { id: "trackSouthIndianInterest", name: "Track South Indian Interest", scope: SCOPE, enabled: true },
      condition: {
        type: "booleanCondition",
        parameterValues: {
          operator: "and", subConditions: [
            { type: "eventTypeCondition", parameterValues: { eventTypeId: "addToCart" } },
            { type: "eventPropertyCondition", parameterValues: { propertyName: "properties.category", comparisonOperator: "equals", propertyValue: "South Indian" } },
          ]
        },
      },
      actions: [{ type: "setPropertyAction", parameterValues: { setPropertyName: "properties.interestedInSouthIndian", setPropertyValue: "true", setPropertyStrategy: "alwaysSet" } }],
    },
    {
      metadata: { id: "trackChineseInterest", name: "Track Chinese Interest", scope: SCOPE, enabled: true },
      condition: {
        type: "booleanCondition",
        parameterValues: {
          operator: "and", subConditions: [
            { type: "eventTypeCondition", parameterValues: { eventTypeId: "addToCart" } },
            { type: "eventPropertyCondition", parameterValues: { propertyName: "properties.category", comparisonOperator: "equals", propertyValue: "Chinese" } },
          ]
        },
      },
      actions: [{ type: "setPropertyAction", parameterValues: { setPropertyName: "properties.interestedInChinese", setPropertyValue: "true", setPropertyStrategy: "alwaysSet" } }],
    },
    {
      metadata: { id: "trackItalianInterest", name: "Track Italian Interest", scope: SCOPE, enabled: true },
      condition: {
        type: "booleanCondition",
        parameterValues: {
          operator: "and", subConditions: [
            { type: "eventTypeCondition", parameterValues: { eventTypeId: "addToCart" } },
            { type: "eventPropertyCondition", parameterValues: { propertyName: "properties.category", comparisonOperator: "equals", propertyValue: "Italian" } },
          ]
        },
      },
      actions: [{ type: "setPropertyAction", parameterValues: { setPropertyName: "properties.interestedInItalian", setPropertyValue: "true", setPropertyStrategy: "alwaysSet" } }],
    },
    {
      metadata: { id: "incrementCartCount", name: "Increment Cart Count", scope: SCOPE, enabled: true },
      condition: { type: "eventTypeCondition", parameterValues: { eventTypeId: "addToCart" } },
      actions: [{ type: "incrementPropertyAction", parameterValues: { propertyName: "totalCartAdds" } }],
    },
    {
      metadata: { id: "trackPurchase", name: "Track Purchase", scope: SCOPE, enabled: true },
      condition: { type: "eventTypeCondition", parameterValues: { eventTypeId: "purchaseComplete" } },
      actions: [{ type: "incrementPropertyAction", parameterValues: { propertyName: "totalPurchases" } }],
    },
    {
      metadata: { id: "incrementPageViews", name: "Increment Page Views", scope: SCOPE, enabled: true },
      condition: { type: "eventTypeCondition", parameterValues: { eventTypeId: "view" } },
      actions: [{ type: "incrementPropertyAction", parameterValues: { propertyName: "pageViewCount" } }],
    },
  ];

  for (const rule of rules) {
    try { await unomiRequest("DELETE", `/cxs/rules/${rule.metadata.id}`); } catch (_) { }
  }
  for (const rule of rules) {
    try {
      const r = await unomiRequest("POST", "/cxs/rules", rule);
      console.log(`[BOOTSTRAP] Rule '${rule.metadata.id}':`, r.ok || r.status === 409 ? "OK" : r.status, r.ok ? "" : JSON.stringify(r.body));
    } catch (e) { console.error(`[BOOTSTRAP] Rule '${rule.metadata.id}':`, e.message); }
  }

  // 4. Segments
  const segments = [
    {
      metadata: { id: "returning-visitor", name: "Returning Visitor", scope: SCOPE, enabled: true },
      condition: { type: "profilePropertyCondition", parameterValues: { propertyName: "properties.pageViewCount", comparisonOperator: "greaterThan", propertyValueInteger: 3 } },
    },
    {
      metadata: { id: "north-indian-lover", name: "North Indian Lover", scope: SCOPE, enabled: true },
      condition: { type: "profilePropertyCondition", parameterValues: { propertyName: "properties.interestedInNorthIndian", comparisonOperator: "equals", propertyValue: "true" } },
    },
    {
      metadata: { id: "south-indian-lover", name: "South Indian Lover", scope: SCOPE, enabled: true },
      condition: { type: "profilePropertyCondition", parameterValues: { propertyName: "properties.interestedInSouthIndian", comparisonOperator: "equals", propertyValue: "true" } },
    },
    {
      metadata: { id: "chinese-food-lover", name: "Chinese Food Lover", scope: SCOPE, enabled: true },
      condition: { type: "profilePropertyCondition", parameterValues: { propertyName: "properties.interestedInChinese", comparisonOperator: "equals", propertyValue: "true" } },
    },
    {
      metadata: { id: "italian-food-lover", name: "Italian Food Lover", scope: SCOPE, enabled: true },
      condition: { type: "profilePropertyCondition", parameterValues: { propertyName: "properties.interestedInItalian", comparisonOperator: "equals", propertyValue: "true" } },
    },
    {
      metadata: { id: "high-value-customer", name: "High Value Customer", scope: SCOPE, enabled: true },
      condition: {
        type: "booleanCondition",
        parameterValues: {
          operator: "and", subConditions: [
            { type: "profilePropertyCondition", parameterValues: { propertyName: "properties.totalPurchases", comparisonOperator: "greaterThan", propertyValueInteger: 0 } },
            { type: "profilePropertyCondition", parameterValues: { propertyName: "properties.totalCartAdds", comparisonOperator: "greaterThan", propertyValueInteger: 3 } },
          ]
        },
      },
    },
  ];

  for (const seg of segments) {
    try {
      const r = await unomiRequest("POST", "/cxs/segments", seg);
      console.log(`[BOOTSTRAP] Segment '${seg.metadata.id}':`, r.ok || r.status === 409 ? "OK" : r.status, r.ok ? "" : JSON.stringify(r.body));
    } catch (e) { console.error(`[BOOTSTRAP] Segment '${seg.metadata.id}':`, e.message); }
  }

  // 5. Scoring plan
  try {
    const r = await unomiRequest("POST", "/cxs/scoring", {
      itemId: "engagementScore", itemType: "scoring",
      metadata: { id: "engagementScore", name: "Engagement Score", scope: SCOPE, enabled: true },
      elements: [
        { condition: { type: "profilePropertyCondition", parameterValues: { propertyName: "properties.pageViewCount", comparisonOperator: "greaterThan", propertyValueInteger: 5 } }, value: 10 },
        { condition: { type: "profilePropertyCondition", parameterValues: { propertyName: "properties.totalCartAdds", comparisonOperator: "greaterThan", propertyValueInteger: 2 } }, value: 25 },
        { condition: { type: "profilePropertyCondition", parameterValues: { propertyName: "properties.interestedInNorthIndian", comparisonOperator: "equals", propertyValue: "true" } }, value: 15 },
        { condition: { type: "profilePropertyCondition", parameterValues: { propertyName: "properties.interestedInSouthIndian", comparisonOperator: "equals", propertyValue: "true" } }, value: 15 },
        { condition: { type: "profilePropertyCondition", parameterValues: { propertyName: "properties.interestedInChinese", comparisonOperator: "equals", propertyValue: "true" } }, value: 15 },
        { condition: { type: "profilePropertyCondition", parameterValues: { propertyName: "properties.interestedInItalian", comparisonOperator: "equals", propertyValue: "true" } }, value: 15 },
        { condition: { type: "profilePropertyCondition", parameterValues: { propertyName: "properties.totalPurchases", comparisonOperator: "greaterThan", propertyValueInteger: 0 } }, value: 40 },
      ],
    });
    console.log("[BOOTSTRAP] Scoring plan:", r.ok || r.status === 409 ? "OK" : r.status, r.ok ? "" : JSON.stringify(r.body));
  } catch (e) { console.error("[BOOTSTRAP] Scoring plan:", e.message); }

  console.log("\n[BOOTSTRAP] Complete.\n");
}

module.exports = { bootstrapUnomi };
