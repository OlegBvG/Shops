import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import com.mongodb.client.model.Aggregates.*;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.eq;

public class Main {

    private static final Logger logger = LogManager.getLogger();
    private static final long SLEEP = 1000;
    static MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);

    static MongoDatabase database = mongoClient.getDatabase("test");

    static MongoCollection<Document> collectionShops = database.getCollection("Shops");
    static MongoCollection<Document> collectionProduct = database.getCollection("Product");
    static MongoCollection<Document> collectionTempShop = database.getCollection("tempShop");

    public static void main(String[] args) throws IOException {

        collectionShops.drop();
        collectionProduct.drop();
        collectionTempShop.drop();

        fillTrading();
       getAllShops();
       System.out.println("\nПродуктов  в магазине Перекресток " + countProductInShop("Перекресток"));
       System.out.println("\nПродуктов  в магазине Алтын " + countProductInShop("Алтын"));
       System.out.println("\nПродуктов  в магазине Пятачок " + countProductInShop("Пятачок"));

       System.out.println("\nСредняя цена продуктов  в магазине Перекресток " + avgProductInShop("Перекресток"));

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

        int results = collectionShops.aggregate(Arrays.asList(match(eq("Name", shopName)),
                unwind("$Product"),
                count("count")
                )).first().getInteger("count");

        return results;
    }

    public static int avgProductInShop(String shopName) {
        AggregateIterable<Document> results =
                collectionShops.aggregate(Arrays.asList(match(eq("Name", shopName)),
                        unwind("$Product"),//возникает ошибка о дублировании ID при обращении к results
                out("tempShop")
        ));

        AggregateIterable<Document> results2 =
                collectionShops.aggregate(Arrays.asList(match(eq("Name", shopName)),
                        lookup("Product", "Product", "Name", "ProductAndPrice"),
//                        unwind("$ProductAndPrice"), //возникает ошибка о дублировании ID  !!!!!!!!!!!
//                        avg("Price", ),      //????? какое выражение указать вторым параметром??? !!!!!!!!!!!!
                        out("tempShop")
                ));



        for (Document d : results2){
            logger.info("\n Продукты в " + shopName + d.get("Product")+ d.get("ProductAndPrice"));

        }

        return 0;
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

    public static boolean addProductToShop(String shopName, String product) {
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

        addProductToShop("Перекресток", "Носки");
        addProductToShop("Перекресток", "Варежки");
        addProductToShop("Перекресток", "Вафли");
        addProductToShop("Перекресток", "Хлеб");
        addProductToShop("Перекресток", "Булки");
        addProductToShop("Перекресток", "Бананы");

        addProductToShop("Алтын", "Носки");
        addProductToShop("Алтын", "Варежки");
        addProductToShop("Алтын", "Вафли");
        addProductToShop("Алтын", "Хлеб");
        addProductToShop("Алтын", "Булки");
        addProductToShop("Алтын", "Бананы");
        addProductToShop("Алтын", "Терка");

        addProductToShop("Пятачок", "Носки");
        addProductToShop("Пятачок", "Варежки");
        addProductToShop("Пятачок", "Вафли");
        addProductToShop("Пятачок", "Хлеб");
        addProductToShop("Пятачок", "Булки");
        addProductToShop("Пятачок", "Бананы");
        addProductToShop("Пятачок", "Диван");


    }
}

