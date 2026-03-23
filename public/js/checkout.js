// ─── Checkout ─────────────────────────────────────────────────────────────────
function openCheckout() {
  if (cart.length === 0) { alert("Your cart is empty!"); return; }
  document.getElementById("cart-drawer").classList.remove("open");
  var total = cart.reduce(function (s, c) { return s + c.price * c.qty; }, 0);
  document.getElementById("checkout-summary").innerHTML =
    cart.map(function (i) { return '<div class="flex justify-between text-gray-600"><span>' + i.name + ' &times; ' + i.qty + '</span><span>$' + (i.price * i.qty).toFixed(2) + '</span></div>'; }).join("") +
    '<div class="border-t border-gray-200 pt-2 mt-2 flex justify-between font-bold text-gray-900"><span>Total</span><span>$' + total.toFixed(2) + '</span></div>';
  document.getElementById("checkout-modal").classList.add("active");
  document.getElementById("overlay").classList.add("visible");
}

function closeCheckout() {
  document.getElementById("checkout-modal").classList.remove("active");
  if (!document.getElementById("cart-drawer").classList.contains("open")) document.getElementById("overlay").classList.remove("visible");
}

function completePurchase() {
  var total = cart.reduce(function (s, c) { return s + c.price * c.qty; }, 0);
  var itemCount = cart.reduce(function (s, c) { return s + c.qty; }, 0);
  var orderId = "ORD-" + Date.now();
  trackPurchase(orderId, total, itemCount);
  refreshContext(1000);
  cart = [];
  updateCartUI();
  renderProducts(currentCategory);
  closeCheckout();
  document.getElementById("overlay").classList.remove("visible");
  var toast = document.getElementById("success-toast");
  toast.classList.remove("translate-y-20", "opacity-0");
  toast.classList.add("translate-y-0", "opacity-100");
  console.log("[Order]", orderId, "$" + total.toFixed(2), itemCount + " items");
  setTimeout(function () { window.location.reload(); }, 2000);
}
