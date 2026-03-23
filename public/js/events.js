// ─── Unomi events ─────────────────────────────────────────────────────────────

// trackPageView: sends an explicit view event so pageViewCount increments via
// the incrementPageViews rule. loadContext() alone does NOT send this in Unomi 2.6.
function trackPageView() {
  if (typeof unomiWebTracker === "undefined") return;
  if (!isTrackingAllowed()) return;
  try {
    unomiWebTracker.collectEvent({
      eventType: "view", scope: "fooddemo",
      source: {
        itemId: "shop-" + document.location.pathname.replace(/\//g, "-"),
        itemType: "page", scope: "fooddemo",
        properties: {
          pageInfo: {
            pageID: "shop-" + document.location.pathname.replace(/\//g, "-"),
            pageName: document.title,
            pagePath: document.location.pathname,
            destinationURL: document.location.origin + document.location.pathname,
            language: "en"
          }
        }
      }
    });
    console.log("[Unomi] view event sent");
  } catch (e) { console.error("[Unomi] view event failed:", e); }
}

function trackProductView(product) {
  if (typeof unomiWebTracker === "undefined") return;
  if (!isTrackingAllowed()) return;
  try {
    unomiWebTracker.collectEvent({
      eventType: "viewProduct", scope: "fooddemo",
      source: { itemId: "shop-main", itemType: "page", scope: "fooddemo" },
      target: { itemId: product.id, itemType: "product", scope: "fooddemo" },
      properties: { productId: product.id, productName: product.name, category: product.category, price: product.price }
    });
  } catch (e) { console.error("[Unomi] viewProduct failed:", e); }
}

function trackAddToCart(product) {
  if (typeof unomiWebTracker === "undefined") return;
  if (!isTrackingAllowed()) return;
  try {
    unomiWebTracker.collectEvent({
      eventType: "addToCart", scope: "fooddemo",
      source: { itemId: "shop-main", itemType: "page", scope: "fooddemo" },
      target: { itemId: product.id, itemType: "product", scope: "fooddemo" },
      properties: { productId: product.id, productName: product.name, category: product.category, price: product.price }
    });
  } catch (e) { console.error("[Unomi] addToCart failed:", e); }
}

function trackRemoveFromCart(productId) {
  if (typeof unomiWebTracker === "undefined") return;
  if (!isTrackingAllowed()) return;
  try {
    unomiWebTracker.collectEvent({
      eventType: "removeFromCart", scope: "fooddemo",
      source: { itemId: "shop-main", itemType: "page", scope: "fooddemo" },
      properties: { productId: productId }
    });
  } catch (e) { console.error("[Unomi] removeFromCart failed:", e); }
}

function trackPurchase(orderId, total, itemCount) {
  if (typeof unomiWebTracker === "undefined") return;
  if (!isOrderTrackingAllowed()) return;
  try {
    unomiWebTracker.collectEvent({
      eventType: "purchaseComplete", scope: "fooddemo",
      source: { itemId: "shop-checkout", itemType: "page", scope: "fooddemo" },
      properties: { orderId: orderId, totalAmount: total, itemCount: itemCount }
    });
  } catch (e) { console.error("[Unomi] purchaseComplete failed:", e); }
}