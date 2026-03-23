// ─── Cart ─────────────────────────────────────────────────────────────────────
function addToCart(productId) {
  var p = PRODUCTS.find(function (x) { return x.id === productId; });
  if (!p) return;
  var ex = cart.find(function (c) { return c.id === productId; });
  if (ex) { ex.qty++; } else { cart.push(Object.assign({}, p, { qty: 1 })); }
  trackAddToCart(p);
  refreshContext(900);
  updateCartUI();
  renderProducts(currentCategory);
  var badge = document.getElementById("cart-count");
  badge.classList.add("badge-pop");
  badge.addEventListener("animationend", function () { badge.classList.remove("badge-pop"); }, { once: true });
}

function removeFromCart(productId) {
  cart = cart.filter(function (c) { return c.id !== productId; });
  trackRemoveFromCart(productId);
  updateCartUI();
  renderProducts(currentCategory);
}

function updateCartUI() {
  var count  = cart.reduce(function (s, c) { return s + c.qty; }, 0);
  var total  = cart.reduce(function (s, c) { return s + c.price * c.qty; }, 0);
  var badge  = document.getElementById("cart-count");
  if (count > 0) { badge.textContent = count; badge.classList.remove("hidden"); } else { badge.classList.add("hidden"); }
  document.getElementById("cart-total").textContent = "$" + total.toFixed(2);
  var listEl = document.getElementById("cart-items-list");
  if (cart.length === 0) { listEl.innerHTML = '<p class="text-center text-gray-400 mt-12 text-sm">Your cart is empty.</p>'; return; }
  listEl.innerHTML = cart.map(function (item) {
    return '<div class="flex items-center gap-4 bg-gray-50 rounded-xl p-3">' +
      '<img src="' + item.imageUrl + '" alt="' + item.name + '" class="w-16 h-12 object-cover rounded-lg"/>' +
      '<div class="flex-1 min-w-0"><p class="text-sm font-semibold text-gray-900 truncate">' + item.name + '</p>' +
      '<p class="text-xs text-gray-400">' + item.category + '</p>' +
      '<p class="text-sm font-bold text-amber-600 mt-0.5">$' + (item.price * item.qty).toFixed(2) + '</p></div>' +
      '<div class="flex flex-col items-end gap-2">' +
      '<button onclick="removeFromCart(\'' + item.id + '\')" class="p-1.5 rounded-lg hover:bg-red-50 text-gray-400 hover:text-red-500 transition">' +
      '<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg></button>' +
      '<span class="text-xs text-gray-400">Qty: ' + item.qty + '</span></div></div>';
  }).join("");
}

function toggleCart() {
  document.getElementById("cart-drawer").classList.toggle("open");
  document.getElementById("overlay").classList.toggle("visible");
}
