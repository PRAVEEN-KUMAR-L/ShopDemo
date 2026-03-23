// ─── Product catalogue ────────────────────────────────────────────────────────
var PRODUCTS = [
  // North Indian
  { id: "ni-001", name: "Butter Chicken", category: "North Indian", price: 349.00, description: "Tender chicken in rich tomato-butter gravy, slow cooked with aromatic spices.", imageUrl: "https://picsum.photos/seed/ni001/300/200", badge: "Popular" },
  { id: "ni-002", name: "Mutton Biryani", category: "North Indian", price: 449.00, description: "Fragrant basmati rice layered with slow-cooked mutton and whole spices.", imageUrl: "https://picsum.photos/seed/ni002/300/200", badge: "New" },
  { id: "ni-003", name: "Dal Makhani", category: "North Indian", price: 249.00, description: "Black lentils simmered overnight in butter and cream. A Punjabi classic.", imageUrl: "https://picsum.photos/seed/ni003/300/200", badge: null },
  { id: "ni-004", name: "Paneer Tikka", category: "North Indian", price: 299.00, description: "Marinated cottage cheese grilled in tandoor with peppers and onions.", imageUrl: "https://picsum.photos/seed/ni004/300/200", badge: "Sale" },

  // South Indian
  { id: "si-001", name: "Masala Dosa", category: "South Indian", price: 199.00, description: "Crispy rice crepe stuffed with spiced potato filling, served with chutneys.", imageUrl: "https://picsum.photos/seed/si001/300/200", badge: "Popular" },
  { id: "si-002", name: "Idli Sambar", category: "South Indian", price: 149.00, description: "Steamed rice cakes served with piping hot vegetable sambar and coconut chutney.", imageUrl: "https://picsum.photos/seed/si002/300/200", badge: null },
  { id: "si-003", name: "Chettinad Chicken", category: "South Indian", price: 399.00, description: "Bold and fiery Chettinad spiced chicken curry from Tamil Nadu.", imageUrl: "https://picsum.photos/seed/si003/300/200", badge: "New" },
  { id: "si-004", name: "Rasam Rice", category: "South Indian", price: 179.00, description: "Tangy tamarind pepper rasam poured over steamed rice. Soul food.", imageUrl: "https://picsum.photos/seed/si004/300/200", badge: "Sale" },

  // Chinese
  { id: "ch-001", name: "Dim Sum Basket", category: "Chinese", price: 299.00, description: "Assorted steamed dumplings — har gow, siu mai and crystal rolls.", imageUrl: "https://picsum.photos/seed/ch001/300/200", badge: "Popular" },
  { id: "ch-002", name: "Kung Pao Chicken", category: "Chinese", price: 349.00, description: "Stir-fried chicken with peanuts, dried chillies and Sichuan pepper.", imageUrl: "https://picsum.photos/seed/ch002/300/200", badge: null },
  { id: "ch-003", name: "Mapo Tofu", category: "Chinese", price: 279.00, description: "Silken tofu in a spicy fermented black bean and chilli sauce.", imageUrl: "https://picsum.photos/seed/ch003/300/200", badge: "New" },
  { id: "ch-004", name: "Spring Rolls", category: "Chinese", price: 199.00, description: "Crispy fried rolls filled with vegetables and glass noodles.", imageUrl: "https://picsum.photos/seed/ch004/300/200", badge: "Sale" },

  // Italian
  { id: "it-001", name: "Margherita Pizza", category: "Italian", price: 399.00, description: "Wood-fired pizza with San Marzano tomatoes, fresh mozzarella and basil.", imageUrl: "https://picsum.photos/seed/it001/300/200", badge: "Popular" },
  { id: "it-002", name: "Spaghetti Carbonara", category: "Italian", price: 449.00, description: "Al dente spaghetti with pancetta, egg yolk, pecorino and black pepper.", imageUrl: "https://picsum.photos/seed/it002/300/200", badge: "New" },
  { id: "it-003", name: "Tiramisu", category: "Italian", price: 249.00, description: "Classic espresso-soaked ladyfinger dessert with mascarpone cream.", imageUrl: "https://picsum.photos/seed/it003/300/200", badge: null },
  { id: "it-004", name: "Mushroom Risotto", category: "Italian", price: 379.00, description: "Creamy arborio rice with wild porcini mushrooms and aged parmesan.", imageUrl: "https://picsum.photos/seed/it004/300/200", badge: "Sale" },
];

// ─── Shared state ─────────────────────────────────────────────────────────────
var cart = [];
var currentCategory = "All";
var viewTimers = {};
window.unomiContext = null;