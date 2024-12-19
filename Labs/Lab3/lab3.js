// Q1. Find orders that are shipped to Denver and have a status “pending”. 
// Return id of such orders only (excluding _id too).
db.orders.find(
    { "shippingAddress.city": "Denver", "status": "pending" },
    { _id: 0, id: "$_id" }
)

/* Output:
  [ { id: ObjectId('605c72d4bcf86cd799439109') } ]
 */


// Q2. Find orders that contain two Bluetooth Speakers (product name). Return id of such orders only.
db.orders.find(
    {
      "items": {
        $elemMatch: {
          "productId": db.products.findOne({ "name": "Bluetooth Speaker" })._id,
          "quantity": 2
        }
      }
    },
    { _id: 0, id: "$_id" }
)

/* Output
  [ { id: ObjectId('605c72d4bcf86cd799439105') } ]
*/


// Q3. Find out, for each category, how many products of that category are in stock.
// Output the name of the category and the number only. No need to consider
// categories which do not have products listed in the products collection.
db.products.aggregate([
  {
    $lookup: {
      from: "categories",
      localField: "category",
      foreignField: "_id",
      as: "categoryInfo"
    }
  },
  {
    $group: {
      _id: "$categoryInfo.name",
      totalStock: { $sum: "$stock" }
    }
  },
  {
    $project: {
      category: "$_id",
      stock: "$totalStock",
      _id: 0
    }
  }
])

/*Output
  [
    { category: [ 'Electronics' ], stock: 600 },
    { category: [ 'Mobile Phones' ], stock: 630 }
  ]
*/


// Q4. Find out names of products whose reviews contain keywords: “great” or “good” (case insensitive). 
// Output names of products and their reviews only.
db.products.aggregate([
  {
    $lookup: {
      from: "reviews",
      localField: "_id",
      foreignField: "productId",
      as: "productReviews"
    }
  },
  {
    $match: {
      "productReviews.review": {
        $regex: /great|good/i
      }
    }
  },
  {
    $project: {
      _id: 0,
      name: 1,
      reviews: "$productReviews.review"
    }
  }
])

/*Output
  [
    {
      name: 'Wireless Headphones',
      reviews: [ 'Great sound quality but a bit pricey.' ]
    },
    {
      name: 'Smartwatch',
      reviews: [ 'Solid smartwatch with great fitness tracking features.' ]
    },
    {
      name: 'Bluetooth Speaker',
      reviews: [ 'Great sound, but the battery could last longer.' ]
    },
    {
      name: 'Digital Camera',
      reviews: [ 'Not great. The camera quality is below expectations.' ]
    },
    {
      name: 'Wireless Charger',
      reviews: [ 'Good value for a wireless charger. Works as advertised.' ]
    }
  ]
 */


// Q5. For each city (that appears in the users collection), 
// find out the number of people who live in the city.
db.users.aggregate([
  {
    $group: {
      _id: "$address.city",
      count: { $sum: 1 }
    }
  },
  { $project: { _id: 0, city: "$_id", population: "$count" } }
])

/*Output
  [
    { city: 'Chicago', population: 1 },
    { city: 'Atlanta', population: 1 },
    { city: 'Los Angeles', population: 1 },
    { city: 'New York', population: 1 },
    { city: 'Seattle', population: 1 },
    { city: 'Houston', population: 1 },
    { city: 'Denver', population: 1 },
    { city: 'Miami', population: 1 },
    { city: 'Phoenix', population: 1 },
    { city: 'San Francisco', population: 1 }
  ]
*/


// Q6. For each different status (e.g., shipped vs pending), 
// find out how many orders are in that status and the total amount of the orders. 
// Output the top-3 statuses ranked by the total amount (higher rank for larger amount).
db.orders.aggregate([
  {
    $group: {
      _id: "$status",
      totalOrders: { $sum: 1 },
      totalAmount: { $sum: "$totalAmount" }
    }
  },
  { $sort: { totalAmount: -1 } },
  { $limit: 3 },
  { $project: { _id: 0, status: "$_id", totalOrders: "$totalOrders", totalAmount: "$totalAmount" } }
])  

/**Output
  [
    { status: 'delivered', totalOrders: 3, totalAmount: 2599.96 },
    { status: 'shipped', totalOrders: 2, totalAmount: 1999.95 },
    { status: 'processing', totalOrders: 1, totalAmount: 1799.98 }
  ]
*/


// Q7. Find orders shipped to Houston where an item has a price between $10 and $50(including) 
// and quantity is greater than 1. Return id of such orders only.
db.orders.find(
  {
    "shippingAddress.city": "Houston",
    "items": {
      $elemMatch: {
        "price": { $gte: 10, $lte: 50 },
        "quantity": { $gt: 1 }
      }
    }
  },
  { _id: 0, id: "$_id" }
)

/**Output
  [ { id: ObjectId('605c72d4bcf86cd799439105') } ]
*/


// Q8.Find out categories which do not have products listed in the products collection. 
// Output the category names in the descending order.
db.categories.aggregate([
  {
    $lookup: {
      from: "products",
      localField: "_id",
      foreignField: "category",
      as: "products"
    }
  },
  { $match: { products: { $size: 0 } } },
  { $sort: { name: -1 } },
  { $project: { _id: 0, name: 1 } }
])

/**Output
  [
    { name: 'Wearables' },
    { name: 'Toys & Games' },
    { name: 'Sports & Outdoors' },
    { name: 'Home Appliances' },
    { name: 'Health & Beauty' },
    { name: 'Footwear' },
    { name: 'Clothing' },
    { name: 'Books' }
  ]
*/


// Q9.For each category (by name), find out the total sales amount of products in that category. 
// No need to consider categories that do not have products.
db.orders.aggregate([
  {
    $unwind: "$items"
  },
  {
    $lookup: {
      from: "products",
      localField: "items.productId",
      foreignField: "_id",
      as: "product"
    }
  },
  {
    $unwind: "$product"
  },
  {
    $lookup: {
      from: "categories",
      localField: "product.category",
      foreignField: "_id",
      as: "category"
    }
  },
  {
    $group: {
      _id: "$category.name",
      totalSales: { $sum: { $multiply: ["$items.price", "$items.quantity"] } }
    }
  },
  { $project: { category: "$_id", totalSales: "$totalSales", _id: 0 } }
])

/**Output
  [
    { category: [ 'Mobile Phones' ], totalSales: 1329.97 },
    { category: [ 'Electronics' ], totalSales: 5999.87 }
  ]
*/


// Q10.Find reviews of electronics (category). Return product descriptions and reviews only.
db.products.aggregate([
  {
    $lookup: {
      from: "categories",
      localField: "category",
      foreignField: "_id",
      as: "category"
    }
  },
  {
    $match: {
      "category.name": "Electronics"
    }
  },
  {
    $lookup: {
      from: "reviews",
      localField: "_id",
      foreignField: "productId",
      as: "reviews"
    }
  },
  { $unwind: "$reviews" },
  { $project: { _id: 0, description: 1, "reviews.review": 1 } }
])

/**Output
  [
    {
      description: 'Bluetooth wireless headphones with noise cancellation.',
      reviews: { review: 'Great sound quality but a bit pricey.' }
    },
    {
      description: 'Fitness and activity tracker with heart rate monitoring.',
      reviews: {
        review: 'Solid smartwatch with great fitness tracking features.'
      }
    },
    {
      description: 'High-performance laptop for gaming and work.',
      reviews: { review: 'Best laptop I have ever used. Highly recommend!' }
    },
    {
      description: 'Portable Bluetooth speaker with 360-degree sound.',
      reviews: { review: 'Great sound, but the battery could last longer.' }
    },
    {
      description: '65-inch 4K Ultra HD TV with HDR support.',
      reviews: { review: 'Fantastic 4K TV! The picture quality is stunning.' }
    },
    {
      description: '24.2MP digital camera with 4K video recording capabilities.',
      reviews: { review: 'Not great. The camera quality is below expectations.' }
    },
    {
      description: 'Next-gen gaming console with 1TB storage.',
      reviews: { review: 'Incredible gaming console! Hours of fun.' }
    }
  ]
*/