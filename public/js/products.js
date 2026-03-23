// ─── Products ─────────────────────────────────────────────────────────────────
function renderProducts(category) {
  var grid = document.getElementById("product-grid");
  var filtered = category === "All" ? PRODUCTS : PRODUCTS.filter(function (p) { return p.category === category; });
  document.getElementById("category-title").textContent = category === "All" ? "All Products" : category;
  document.getElementById("product-count").textContent = filtered.length + " product" + (filtered.length !== 1 ? "s" : "");
  grid.innerHTML = filtered.map(function (p) {
    var badge = p.badge ? '<span class="absolute top-3 left-3 px-2.5 py-1 rounded-full text-[11px] font-bold uppercase tracking-wide ' + (p.badge === "New" ? "bg-blue-500 text-white" : p.badge === "Sale" ? "bg-red-500 text-white" : "bg-amber-400 text-white") + '">' + p.badge + '</span>' : "";
    var inCart = !!cart.find(function (c) { return c.id === p.id; });
    return '<article class="product-card bg-white rounded-2xl overflow-hidden shadow-sm border border-gray-100 flex flex-col" onmouseenter="startViewTimer(\'' + p.id + '\')" onmouseleave="clearViewTimer(\'' + p.id + '\')">' +
      '<div class="relative overflow-hidden bg-gray-100"><img src="' + p.imageUrl + '" alt="' + p.name + '" class="w-full h-48 object-cover transition-transform duration-500 hover:scale-105" loading="lazy"/>' +
      badge + '<div class="absolute top-3 right-3 px-2 py-0.5 bg-white/80 backdrop-blur-sm rounded-full text-xs font-semibold text-gray-600 border border-gray-200">' + p.category + '</div></div>' +
      '<div class="p-5 flex flex-col flex-1"><h3 class="font-semibold text-base text-gray-900 mb-1 leading-snug">' + p.name + '</h3>' +
      '<p class="text-xs text-gray-400 flex-1 mb-4 leading-relaxed">' + p.description + '</p>' +
      '<div class="flex items-center justify-between"><span class="text-lg font-bold text-brand">$' + p.price.toFixed(2) + '</span>' +
      '<button onclick="addToCart(\'' + p.id + '\')" class="px-4 py-2 rounded-xl text-sm font-semibold transition ' + (inCart ? "bg-green-500 text-white cursor-default" : "bg-amber-500 hover:bg-amber-600 text-white") + '">' + (inCart ? "&#x2713; Added" : "Add to Cart") + '</button></div></div></article>';
  }).join("");
}

function startViewTimer(productId) {
  if (viewTimers[productId]) return;
  viewTimers[productId] = setTimeout(function () {
    var p = PRODUCTS.find(function (x) { return x.id === productId; });
    if (p) trackProductView(p);
    delete viewTimers[productId];
  }, 2000);
}

function clearViewTimer(productId) {
  if (viewTimers[productId]) { clearTimeout(viewTimers[productId]); delete viewTimers[productId]; }
}
