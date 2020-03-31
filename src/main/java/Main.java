import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BsonField;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lte;

public class Main {

    private static final Logger logger = LogManager.getLogger();
    static MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);

    static MongoDatabase database = mongoClient.getDatabase("test");

    static MongoCollection<Document> collectionShops = database.getCollection("Shops");
    static MongoCollection<Document> collectionProduct = database.getCollection("Product");
    static MongoCollection<Document> collectionTempShop = database.getCollection("tempShop");

    static String marks;
    static Scanner in = new Scanner(System.in);
    final static String REGEX_ADD_SHOP = "^(?<comand>(ДОБАВИТЬ_МАГАЗИН)\\s+(?<nameShop>[А-Яа-я]+)$)";
    final static String REGEX_ADD_PRODUCT = "^(?<comand>(ДОБАВИТЬ_ТОВАР)\\s+(?<nameProduct>[А-Яа-я_]+)\\s+(?<priceProduct>[0-9,?|.?]+)$)";
    final static String REGEX_ADD_PRODUCT_TO_SHOP = "^(?<comand>(ВЫСТАВИТЬ_ТОВАР)\\s+(?<nameProduct>[А-Яа-я_]+)\\s+(?<nameShop>[А-Яа-я_]+)$)";
    final static String REGEX_STATISTICS = "^(?<comand>(СТАТИСТИКА_ТОВАРОВ)\\s+(?<nameShop>[А-Яа-я]+)$)";
    final static String REGEX_HELP = "^HELP$";
    final static String REGEX_EXIT = "^EXIT$";

    public static void main(String[] args) throws IOException {

        collectionShops.drop();
        collectionProduct.drop();
        collectionTempShop.drop();

        fillTrading();

        System.out.println("\nДоступны следующие команды: ДОБАВИТЬ_МАГАЗИН; ДОБАВИТЬ_ТОВАР; ВЫСТАВИТЬ_ТОВАР; СТАТИСТИКА_ТОВАРОВ");
        System.out.println("дополнительные команды HELP, EXIT: \n");

        while (true) {
            System.out.println("\nВведите КОМАНДУ: \n");
            marks = in.nextLine().trim();

            if (marks.matches(REGEX_ADD_SHOP)) {
                String[] parameter = marks.split(" ");
                addShop(parameter[1]);

            } else if (marks.matches(REGEX_EXIT)) {
                exit();

            } else if (marks.matches(REGEX_HELP)) {
                help();

            } else if (marks.matches(REGEX_ADD_PRODUCT)) {
                String[] parameter = marks.split(" ");
                addProduct(parameter[1], Double.parseDouble( parameter[2].replace(",",".") ));
                continue;

            } else if (marks.matches(REGEX_ADD_PRODUCT_TO_SHOP)) {
                String[] parameter = marks.split(" ");
                addProductToShop(parameter[1], parameter[2]);
                continue;

            } else if (marks.matches(REGEX_STATISTICS)) {
                String[] parameter = marks.split(" ");
                getAllShops();

                //  — Общее количество товаров
                System.out.println("\nПродуктов  в магазине " + parameter[1] + " = " + countProductInShop(parameter[1]) + " шт.");

               //— Среднюю цену товара
                System.out.println("\nСредняя цена продуктов  в магазине " + parameter[1] + " = " + avgProductInShop(parameter[1]) + " руб.");

               //— Самый дорогой и самый дешевый товар
                System.out.println("\nСамый дорогой продукт  в магазине " + parameter[1] + " => "
                        + expensiveProductInShop(parameter[1]).entrySet().iterator().next().getKey()
                        + " = "
                        + expensiveProductInShop(parameter[1]).entrySet().iterator().next().getValue() + " руб.");

                System.out.println("\nСамый дешевый продукт  в магазине " + parameter[1] + " => "
                        + cheapProductInShop(parameter[1]).entrySet().iterator().next().getKey()
                        + " = "
                        + cheapProductInShop(parameter[1]).entrySet().iterator().next().getValue() + " руб.");

                //— Количество товаров, дешевле 100 рублей.
                double priceHigh = 100;
                System.out.println("\nПродуктов  в магазине " + parameter[1] + " дешевле " + priceHigh +  " руб.: "
                        + countProductInShopCheaper(parameter[1], priceHigh) + " наименования");

                continue;

            } else {
                System.out.println("Неверно введена команда. Попробуйте еще раз!");
            }
        }
    }

    private static void help() throws IOException {
        String infoFile = "d:\\Skill\\IdeaProjects\\Shops\\src\\main\\resources\\info.txt";
        InputStream input = new BufferedInputStream(new FileInputStream(infoFile));
        byte[] buffer = new byte[8192];

        try {
            for (int length = 0; (length = input.read(buffer)) != -1;) {
                System.out.write(buffer, 0, length);
            }
        } finally {
            input.close();
        }
    }

    private static void exit() {
    System.exit(0);
}

    public static int countProductInShopOld(String shopName) {

        var query = new BasicDBObject("Name",
                new BasicDBObject("$eq", shopName));
        FindIterable fit = collectionShops.find(query).limit(1);

        var shop = new HashSet<Document>();
        fit.into(shop);

        for (Document s : shop) {

            ArrayList ProductAll = (ArrayList) s.get("Product");
            return ProductAll.size();
        }
        return 0;
    }

    public static int countProductInShop(String shopName) {

        int countProduct = collectionShops.aggregate(Arrays.asList(match(eq("Name", shopName)),
                unwind("$Product"),
                count("count")
                )).first().getInteger("count");

        return countProduct;
    }

    public static int countProductInShopCheaper(String shopName, double priceHigh) {
        collectionProduct.aggregate(Arrays.asList(match(lte("Price", priceHigh)),
                out("tempProductCheaper")
        ));

            int countProduct =
                    collectionShops.aggregate(Arrays.asList(match(eq("Name", shopName)),
                            lookup("tempProductCheaper", "Product", "Name", "ProductAndPrice"),
                            unwind("$ProductAndPrice"),
                            count(),
                            out("tempShopSum2")
                    )).first().getInteger("count");

            return countProduct;
    }


    public static Map<String, Double> expensiveProductInShop(String shopName) {
        double maxPrice =
                collectionShops.aggregate(Arrays.asList(match(eq("Name", shopName)),
                        lookup("Product", "Product", "Name", "ProductAndPrice"),
                        unwind("$ProductAndPrice"),
                        group("_id", new BsonField("max", new BsonDocument("$max", new BsonString("$ProductAndPrice.Price")))),
                        out("tempShopMax")
                ))
        .first().getDouble("max");

        String maxPriceProduct =
               collectionProduct.aggregate(Arrays.asList(match(eq("Price", maxPrice)),
                out("maxPriceProduct")
               ))
                .first().getString("Name");

        Map<String, Double> maxPriceProductInShop = new HashMap<String, Double>();
        maxPriceProductInShop.put(maxPriceProduct, maxPrice);

        return maxPriceProductInShop;
    }

    public static Map<String, Double> cheapProductInShop(String shopName) {
        double minPrice =
                collectionShops.aggregate(Arrays.asList(match(eq("Name", shopName)),
                        lookup("Product", "Product", "Name", "ProductAndPrice"),
                        unwind("$ProductAndPrice"),
                        group("_id", new BsonField("min", new BsonDocument("$min", new BsonString("$ProductAndPrice.Price")))),
                        out("tempShopMax")
                ))
                        .first().getDouble("min");

        String minPriceProduct =
                collectionProduct.aggregate(Arrays.asList(match(eq("Price", minPrice)),
                        out("minPriceProduct")
                ))
                        .first().getString("Name");

        Map<String, Double> minPriceProductInShop = new HashMap<String, Double>();
        minPriceProductInShop.put(minPriceProduct, minPrice);

        return minPriceProductInShop;
    }

    public static double avgProductInShop(String shopName) {
        double avgPrice =
                collectionShops.aggregate(Arrays.asList(match(eq("Name", shopName)),
                        lookup("Product", "Product", "Name", "ProductAndPrice"),
                        unwind("$ProductAndPrice"),
                        group("_id", new BsonField("averageCost", new BsonDocument("$avg", new BsonString("$ProductAndPrice.Price")))),
                        out("tempShop")
                )).first().getDouble("averageCost");

        return avgPrice;
    }



    public static void getAllShops() {
        FindIterable fit = collectionShops.find();

        var allShops = new HashSet<Document>();
        fit.into(allShops);

        for (Document shop : allShops) {
            logger.info("\nМагазин ===>  " + shop.getString("Name"));
            ArrayList ProductAll = (ArrayList) shop.get("Product");
            for (int i = 0; i < ProductAll.size(); i++)
                logger.info("\tТовар ===>  " + ProductAll.get(i));
        }
    }

    public static HashSet<Document> getShopByName(String shopName) {

        var query = new BasicDBObject("Name",
                new BasicDBObject("$eq", shopName));
        FindIterable fit = collectionShops.find(query);

        var docs = new HashSet<Document>();
        fit.into(docs);

        return docs;
    }

    public static boolean isShopName(String shopName) {

        var query = new BasicDBObject("Name",
                new BasicDBObject("$eq", shopName));
        FindIterable fit = collectionShops.find(query);

        if (fit.iterator().hasNext()) {
            return true;
        }
        return false;
    }

    public static boolean addShop(String shopName) {
        if (!isShopName(shopName)) {
            collectionShops.insertOne(new Document()
                    .append("Name", shopName)
                    .append("Product", new HashSet<>()));
            return true;
        }
        return false;

    }

    public static boolean addProductToShop(String product, String shopName) {
        if (!isProductName(product)) return false;

        if (!isShopName(shopName)) return false;

        HashSet<Document> documentFind = getShopByName(shopName);
        for (Document doc : documentFind) {
           ArrayList<String> productInShop = new ArrayList<String>((ArrayList) doc.get("Product"));

            if (productInShop == null || !productInShop.contains(product)) {
                productInShop.add(product);
                collectionShops.updateOne(eq("Name", shopName)
                        , new Document("$set", new Document("Name", shopName).append("Product", productInShop)));
                return true;
            }
        }
        return false;
    }

    public static boolean addProduct(String productName, double price) {
        if (!isProductName(productName)) {
            collectionProduct.insertOne(new Document()
                    .append("Name", productName)
                    .append("Price", price));
            return true;

        } else {
            collectionProduct.updateOne(eq("Name", productName)
                    , new Document("$set", new Document("Name", productName).append("Price", price)));
        }
        return false;
    }

    public static boolean isProductName(String productName) {
        var query = new BasicDBObject("Name",
                new BasicDBObject("$eq", productName));
        FindIterable fit = collectionProduct.find(query);
        if (fit.iterator().hasNext()) {
            return true;
        }
        return false;
    }

    public static void fillTrading(){
        addShop("Перекресток");
        addShop("Алтын");
        addShop("Пятачок");

        addProduct("Носки", 65.2);
        addProduct("Варежки", 6.12);
        addProduct("Вафли", 15.2);
        addProduct("Хлеб", 5.7);
        addProduct("Булки", 6.12);
        addProduct("Бананы", 25.3);
        addProduct("Терка", 155.3);
        addProduct("Диван", 2500);
        addProduct("Диван", 3500);

        addProductToShop("Носки", "Перекресток");
        addProductToShop("Варежки", "Перекресток");
        addProductToShop("Вафли", "Перекресток");
        addProductToShop("Хлеб", "Перекресток");
        addProductToShop("Булки", "Перекресток");
        addProductToShop("Бананы", "Перекресток");

        addProductToShop("Носки", "Алтын");
        addProductToShop("Варежки", "Алтын");
        addProductToShop("Вафли", "Алтын");
        addProductToShop("Хлеб", "Алтын");
        addProductToShop("Булки", "Алтын");
        addProductToShop("Бананы", "Алтын");
        addProductToShop("Терка", "Алтын");

        addProductToShop("Носки", "Пятачок");
        addProductToShop("Варежки", "Пятачок");
        addProductToShop("Вафли", "Пятачок");
        addProductToShop("Хлеб", "Пятачок");
        addProductToShop("Булки", "Пятачок");
        addProductToShop("Бананы", "Пятачок");
        addProductToShop("Диван", "Пятачок");


    }
}

