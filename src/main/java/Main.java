import utils.ConnectConfig;
import utils.DatabaseConnector;

import java.util.Scanner;
import java.util.logging.Logger;

import entities.Book;

public class Main {

    private static final Logger log = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        try {
            // parse connection config from "resources/application.yaml"
            ConnectConfig conf = new ConnectConfig();
            log.info("Success to parse connect config. " + conf.toString());
            // connect to database
            DatabaseConnector connector = new DatabaseConnector(conf);
            boolean connStatus = connector.connect();
            if (!connStatus) {
                log.severe("Failed to connect database.");
                System.exit(1);
            }
            /* do somethings */
            System.out.println("enter your choice");
            System.out.println("1. store book");
            System.out.println("2. increase book stock");
            System.out.println("3. batch store books");
            System.out.println("4. remove book");
            System.out.println("1. store book");
            System.out.println("5. modify book info");
            System.out.println("6. query book");
            System.out.println("7. borrow book");
            System.out.println("8. return book");
            System.out.println("9. show borrow history");
            System.out.println("10. register card");
            System.out.println("11. remove card");
            System.out.println("12. show cards");

            int choice;
            Scanner scanner = new Scanner(System.in);
            choice = scanner.nextInt();
            // if(choice==1){
            //     storeBook(Book(scanner.nextLine()));
            // }
            // release database connection handler
            if (connector.release()) {
                log.info("Success to release connection.");
            } else {
                log.warning("Failed to release connection.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
