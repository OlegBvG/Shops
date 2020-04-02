import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BsonField;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

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

    //--------------------

    private static final String STORES_COLLECTION_NAME = "Stores";
    private static final String GOODS_COLLECTION_NAME = "Products";

    private static final String STORE_FIELD_ID = "_id";
    private static final String STORE_FIELD_NAME = "Name";
    private static final String STORE_FIELD_GOODS = "Product";

    private static final String GOODS_FIELD_ID = "_id";
    private static final String GOODS_FIELD_NAME = "Name";
    private static final String GOODS_FIELD_PRICE = "Price";
    //-------------------------

    static MongoCollection<Document> collectionShops = database.getCollection(STORES_COLLECTION_NAME);
    static MongoCollection<Document> collectionProduct = database.getCollection(GOODS_COLLECTION_NAME);
//    static MongoCollection<Document> collectionTempShop = database.getCollection("tempShop");

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
//        collectionTempShop.drop();

        fillTrading();
//        statistic("Перекресток");

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
//                getAllShops();

                statistic(parameter[1]);


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

        var query = new BasicDBObject(STORE_FIELD_NAME,
                new BasicDBObject("$eq", shopName));
        FindIterable fit = collectionShops.find(query);

        var docs = new HashSet<Document>();
        fit.into(docs);

        return docs;
    }

    public static boolean isShopName(String shopName) {

        var query = new BasicDBObject(STORE_FIELD_NAME,
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
                    .append(STORE_FIELD_NAME, shopName)
                    .append(STORE_FIELD_GOODS, new HashSet<>()));
            return true;
        }
        return false;

    }

    public static boolean addProductToShop(String product, String shopName) {
        if (!isProductName(product)) return false;

        if (!isShopName(shopName)) return false;

        HashSet<Document> documentFind = getShopByName(shopName);
        for (Document doc : documentFind) {
           ArrayList<String> productInShop = new ArrayList<String>((ArrayList) doc.get(STORE_FIELD_GOODS));

            if (productInShop == null || !productInShop.contains(product)) {
                productInShop.add(product);
                collectionShops.updateOne(eq(STORE_FIELD_NAME, shopName)
                        , new Document("$set", new Document(STORE_FIELD_NAME, shopName).append(STORE_FIELD_GOODS, productInShop)));
                return true;
            }
        }
        return false;
    }

    public static boolean addProduct(String productName, double price) {
        if (!isProductName(productName)) {
            collectionProduct.insertOne(new Document()
                    .append(GOODS_FIELD_NAME, productName)
                    .append(GOODS_FIELD_PRICE, price));
            return true;

        } else {
            collectionProduct.updateOne(eq(GOODS_FIELD_NAME, productName)
                    , new Document("$set", new Document(GOODS_FIELD_NAME, productName).append(GOODS_FIELD_PRICE, price)));
        }
        return false;
    }

    public static boolean isProductName(String productName) {
        var query = new BasicDBObject(GOODS_FIELD_NAME,
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

    public static void statistic(String shopName) {

        //создаем документ lookup в который принимаем товары из коллекции магазина которые
// хранятся в STORE_FIELD_GOODS
// и получаем связь с коллекций GOODS_COLLECTION_NAME и называем это goodsRef
        List<Bson> aggregations = new ArrayList<>();
        aggregations.add(match(eq(STORE_FIELD_NAME, shopName)));
        aggregations.add(
                new Document("$lookup",
                        new Document()
                                .append("localField", STORE_FIELD_GOODS)
                                .append("foreignField", GOODS_FIELD_NAME)
                                .append("from", GOODS_COLLECTION_NAME)
                                .append("as", "goodsRef")
                )
        );

        // разворачиваем список товаров
        aggregations.add(
                new Document("$unwind", new Document("path", "$goodsRef"))
        );

        // тут мы получаем товары меньше 100
// в переменную список priceIsLessThan100 группируя по имени магазина
// добавляем условие того что Price (тут промахнулся и не вставил константу)
// меньше 100 используя оператор lt (less then)
        aggregations.add(
                new Document("$project",
                        new Document()
                                .append(STORE_FIELD_NAME, 1)
                                .append("goodsRef", 1)
                                .append("priceIsLessThan100",
                                        new Document("$cond",
                                                new Document()
                                                        .append("if", new Document("$lt", Arrays.asList("$goodsRef.Price", 100)))
                                                        .append("then", 1)
                                                        .append("else", 0)
                                        )
                                )
                )
        );

        // остальная агрегация уже проще
        // получаем имя магазина из агрегации, получаем среднее значение $avg на основе цен товаров
        // и прочие параметры также суммируем количество товаров меньше чем 100 руб
        aggregations.add(
                new Document("$group",
                        new Document()
                                .append(STORE_FIELD_ID, "$" + STORE_FIELD_NAME)
                                .append("avg", new Document("$avg", "$goodsRef." + GOODS_FIELD_PRICE))
                                .append("min", new Document("$min", "$goodsRef." + GOODS_FIELD_PRICE))
                                .append("max", new Document("$max", "$goodsRef." + GOODS_FIELD_PRICE))
                                .append("count", new Document("$sum", 1))
                                .append("countWherePriceIsLessThan100", new Document("$sum", "$priceIsLessThan100"))
                )
        );

        // это уже применение агрегации и получение данных и их вывод
        try {
            for (Document doc : collectionShops.aggregate(aggregations)) {
                System.out.printf("--- %s%n %s%n %s%n %s%n %s%n %s%n",
                       "Магазин - " + doc.getString(STORE_FIELD_ID),
                       "Средняя цена продуктов: " + doc.getDouble("avg"),
                       "Минимальная цена продукта: " + doc.getDouble("min"),
                       "Максимальная цена продукта: " +  doc.getDouble("max"),
                       "Количество продуктов: " +  doc.getInteger("count"),
                       "Количество продуктов дешевле 100 руб.: " +  doc.getInteger("countWherePriceIsLessThan100")
                );
            }
        } catch (
                MongoException exception) {
            exception.printStackTrace();
        }
    }
}

