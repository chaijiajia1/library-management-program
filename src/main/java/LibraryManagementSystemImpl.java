import entities.Book;
import entities.Borrow;
import entities.Card;
import entities.Card.CardType;
import queries.*;
import queries.BorrowHistories.Item;
import utils.DBInitializer;
import utils.DatabaseConnector;

import java.sql.*;
import java.util.List;
import java.util.*;
public class LibraryManagementSystemImpl implements LibraryManagementSystem {

    private final DatabaseConnector connector;

    public LibraryManagementSystemImpl(DatabaseConnector connector) {
        this.connector = connector;
    }

    @Override
    public ApiResult storeBook(Book book) {
        Connection conn = connector.getConn();
        //test if the book already exists
        try {
            PreparedStatement stmt = conn.prepareStatement("SELECT * FROM book WHERE category=? AND title=? AND press=? AND publish_year=? AND author=? AND price=? AND stock=?");
            stmt.setString(1, book.getCategory());
            stmt.setString(2, book.getTitle());
            stmt.setString(3, book.getPress());
            stmt.setInt(4, book.getPublishYear());
            stmt.setString(5, book.getAuthor());
            stmt.setDouble(6, book.getPrice());
            stmt.setInt(7, book.getStock());
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {//if exist
                return new ApiResult(false, "Book already exists");
            }
        } catch (Exception e) {
            return new ApiResult(false, e.getMessage());
        }
        //insert the book
        try {
            PreparedStatement stmt = conn.prepareStatement("INSERT INTO book (category,title,press,publish_year,author,price,stock) VALUES ( ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);//最后一句话是返回主键
            stmt.setString(1, book.getCategory());
            stmt.setString(2, book.getTitle());
            stmt.setString(3, book.getPress());
            stmt.setInt(4, book.getPublishYear());
            stmt.setString(5, book.getAuthor());
            stmt.setDouble(6, book.getPrice());
            stmt.setInt(7, book.getStock());
            stmt.executeUpdate();//得到记录的条数
            //得到自动生成的主键
            ResultSet rs = stmt.getGeneratedKeys();
            if (rs.next()) {
                book.setBookId(rs.getInt(1));//返回得到的主键给book
            }
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, "store book successfully");
    }

    @Override
    public ApiResult incBookStock(int bookId, int deltaStock) {
        Connection conn = connector.getConn();
        try {
            PreparedStatement stmt = conn.prepareStatement("SELECT * FROM book WHERE book_id=?");//find the book
            stmt.setInt(1, bookId);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {//if not exist
                return new ApiResult(false, "inc book stock failed because no book exist");
            }
            int stock = rs.getInt("stock");
            stock += deltaStock;
            if (stock < 0) {
                return new ApiResult(false, "Stock is not enough");
            }
            stmt = conn.prepareStatement("UPDATE book SET stock=? WHERE book_id=?");
            stmt.setInt(1, stock);
            stmt.setInt(2, bookId);
            stmt.executeUpdate();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, "inc book stock successfully");
    }

    @Override
    public ApiResult storeBook(List<Book> books) {
        Connection conn = connector.getConn();
        try {
            PreparedStatement stmt = conn.prepareStatement("INSERT INTO book (category,title,press,publish_year,author,price,stock) VALUES ( ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            for (Book book : books) {//遍历list中的所有book
                PreparedStatement stmt1 = conn.prepareStatement("SELECT * FROM book WHERE category=? AND title=? AND press=? AND publish_year=? AND author=? AND price=? AND stock=?");
                stmt1.setString(1, book.getCategory());
                stmt1.setString(2, book.getTitle());
                stmt1.setString(3, book.getPress());
                stmt1.setInt(4, book.getPublishYear());
                stmt1.setString(5, book.getAuthor());
                stmt1.setDouble(6, book.getPrice());
                stmt1.setInt(7, book.getStock());
                ResultSet rs = stmt1.executeQuery();
                if (rs.next()) {//if exist
                    return new ApiResult(false, "Book already exists");
                }
                stmt.setString(1, book.getCategory());
                stmt.setString(2, book.getTitle());
                stmt.setString(3, book.getPress());
                stmt.setInt(4, book.getPublishYear());
                stmt.setString(5, book.getAuthor());
                stmt.setDouble(6, book.getPrice());
                stmt.setInt(7, book.getStock());
                stmt.addBatch();//组装到一起
            }
            stmt.executeBatch();//一起执行
            ResultSet rs = stmt.getGeneratedKeys();//得到自动生成的主键
            int i = 0;//list中的第i本书
            while (rs.next()) {
                books.get(i).setBookId(rs.getInt(1));//主键总共只有一位
                i++;
            }
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, "store books successfully");
    }

    @Override
    public ApiResult removeBook(int bookId) {
        Connection conn = connector.getConn();
        try {
            PreparedStatement stmt = conn.prepareStatement("SELECT * FROM book WHERE book_id=?");
            stmt.setInt(1, bookId);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {//if not exist
                return new ApiResult(false, "Book does not exist");
            }
            //test whether the book had been borrowed
            stmt = conn.prepareStatement("SELECT * FROM borrow WHERE book_id=? and return_time=0");
            stmt.setInt(1, bookId);
            rs = stmt.executeQuery();
            if (rs.next()) {//if exist
                return new ApiResult(false, "Book has been borrowed");
            } 
            stmt = conn.prepareStatement("DELETE FROM book WHERE book_id=?");
            stmt.setInt(1, bookId);
            stmt.executeUpdate();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, "remove book successfully");
    }

    @Override
    public ApiResult modifyBookInfo(Book book) {
        Connection conn = connector.getConn();
        try {
            PreparedStatement stmt = conn.prepareStatement("SELECT * FROM book WHERE book_id=?");
            stmt.setInt(1, book.getBookId());
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {//if not exist
                return new ApiResult(false, "Book does not exist");
            }
            stmt = conn.prepareStatement("UPDATE book SET category=?,title=?,press=?,publish_year=?,author=?,price=? WHERE book_id=?");
            stmt.setString(1, book.getCategory());
            stmt.setString(2, book.getTitle());
            stmt.setString(3, book.getPress());
            stmt.setInt(4, book.getPublishYear());
            stmt.setString(5, book.getAuthor());
            stmt.setDouble(6, book.getPrice());
            stmt.setInt(7, book.getBookId());
            stmt.executeUpdate();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, "modify book info successfully");
    }

    
    @Override
    public ApiResult queryBook(BookQueryConditions conditions) {
        Connection conn = connector.getConn();
        String sort_by= conditions.getSortBy().getValue();
        String sort_order= conditions.getSortOrder().getValue();
        ResultSet rs[] = new ResultSet[7],result=null;
        List <Book> bookList = new ArrayList<>();
        int flag[]=new int[7];
        for(int i=0;i<7;i++){
            flag[i]=0;
        }
        try{
            if(conditions.getCategory()!=null){
                flag[0]=1;
                PreparedStatement stmt = (sort_by=="book_id"? conn.prepareStatement("SELECT * FROM book WHERE category=? ORDER BY "+sort_by+" "+sort_order): conn.prepareStatement("SELECT * FROM book WHERE category=? ORDER BY "+sort_by+" "+sort_order +" ,book_id ASC"));//这里一定要把sort_by 和sort_order 单独分开，否则直接用？的话，sql语句中会带有引号
                stmt.setString(1, conditions.getCategory());
                rs[0] = stmt.executeQuery();
            }
            if(conditions.getTitle()!=null){
                flag[1]=1;
                PreparedStatement stmt = (sort_by=="book_id"? conn.prepareStatement("SELECT * FROM book WHERE title=? ORDER BY "+sort_by+" "+sort_order): conn.prepareStatement("SELECT * FROM book WHERE title=? ORDER BY "+sort_by+" "+sort_order +" ,book_id ASC"));
                stmt.setString(1, conditions.getTitle());
                rs[1] = stmt.executeQuery();
            }
            if(conditions.getPress()!=null){
                flag[2]=1;
                String presspattern=conditions.getPress();
                PreparedStatement stmt = (sort_by=="book_id"? conn.prepareStatement("SELECT * FROM book WHERE (press like '%"+presspattern+"%')  ORDER BY "+sort_by+" "+sort_order): conn.prepareStatement("SELECT * FROM book WHERE (press like '%"+presspattern+"%') ORDER BY "+sort_by+" "+sort_order +" ,book_id ASC"));
                rs[2] = stmt.executeQuery();
            }
            if(conditions.getMaxPublishYear()!=null||conditions.getMinPublishYear()!=null){
                flag[3]=1;
                PreparedStatement stmt = (sort_by=="book_id"?conn.prepareStatement("SELECT * FROM book WHERE publish_year>=? AND publish_year<=? order by "+sort_by+" "+sort_order):conn.prepareStatement("SELECT * FROM book WHERE publish_year>=? AND publish_year<=? order by "+sort_by+" "+sort_order +" ,book_id ASC"));
                stmt.setInt(1, (conditions.getMinPublishYear()!=null)?conditions.getMinPublishYear():0);
                stmt.setInt(2, (conditions.getMaxPublishYear()!=null)?conditions.getMaxPublishYear():999999);
                rs[3] = stmt.executeQuery();
            }
            if(conditions.getAuthor()!=null){
                flag[4]=1;
                String presspattern=conditions.getAuthor();
                PreparedStatement stmt = (sort_by=="book_id"? conn.prepareStatement("SELECT * FROM book WHERE (author like '%"+presspattern+"%')  ORDER BY "+sort_by+" "+sort_order): conn.prepareStatement("SELECT * FROM book WHERE (author like '%"+presspattern+"%') ORDER BY "+sort_by+" "+sort_order +" ,book_id ASC"));
                rs[4] = stmt.executeQuery();
            }
            if(conditions.getMaxPrice()!=null||conditions.getMinPrice()!=null){
                flag[5]=1;
                PreparedStatement stmt = (sort_by=="book_id"?conn.prepareStatement("SELECT * FROM book WHERE price>=? AND price<=? order by "+sort_by+" "+sort_order):conn.prepareStatement("SELECT * FROM book WHERE price>=? AND price<=? order by "+sort_by+" "+sort_order +" ,book_id ASC"));
                stmt.setDouble(1, (conditions.getMinPrice()==null)?0:conditions.getMinPrice());
                stmt.setDouble(2, (conditions.getMaxPrice()==null)?999999:conditions.getMaxPrice());
                rs[5] = stmt.executeQuery();
            }
            if(flag[0]==0&&flag[1]==0&&flag[2]==0&&flag[3]==0&&flag[4]==0&&flag[5]==0){
                flag[6]=1;
                PreparedStatement stmt = (sort_by=="book_id"? conn.prepareStatement("SELECT * FROM book order by "+sort_by+" "+sort_order): conn.prepareStatement("SELECT * FROM book order by "+sort_by+" "+sort_order +" ,book_id ASC"));
                rs[6] = stmt.executeQuery();
            }
            int first=0;
            for(int i=0;i<7;i++){
                if(flag[i]==1){
                    if(first==0){//第一个
                        while(rs[i].next()){//这里顺便把空集也考虑了
                            Book book = new Book();
                            book.setBookId(rs[i].getInt("book_id"));
                            book.setCategory(rs[i].getString("category"));
                            book.setTitle(rs[i].getString("title"));
                            book.setPress(rs[i].getString("press"));
                            book.setPublishYear(rs[i].getInt("publish_year"));
                            book.setAuthor(rs[i].getString("author"));
                            book.setPrice(rs[i].getDouble("price"));
                            book.setStock(rs[i].getInt("stock"));
                            bookList.add(book);
                        }
                        first=1;
                    }
                    else{//第二个及以后,在前面的表中去除不在后面表中的元素
                        List<Book> bookList2=new ArrayList<Book>();
                        while(rs[i].next()){//这里顺便把空集也考虑了
                            Book book = new Book();
                            book.setBookId(rs[i].getInt("book_id"));
                            book.setCategory(rs[i].getString("category"));
                            book.setTitle(rs[i].getString("title"));
                            book.setPress(rs[i].getString("press"));
                            book.setPublishYear(rs[i].getInt("publish_year"));
                            book.setAuthor(rs[i].getString("author"));
                            book.setPrice(rs[i].getDouble("price"));
                            book.setStock(rs[i].getInt("stock"));
                            bookList2.add(book);
                        }
                        Iterator<Book> it = bookList.iterator();
                        while(it.hasNext()){
                            Book book = it.next();
                            int flag2=0;
                            for(int j=0;j<bookList2.size();j++){
                                if(book.getBookId()==bookList2.get(j).getBookId()){
                                    flag2=1;
                                    break;
                                }
                            }
                            if(flag2==0){
                                it.remove();
                            }
                        }
                    }
                }
            }
        }catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        
        return new ApiResult(true, "inc book stock successfully", new BookQueryResults(bookList) );
    }

    
    @Override
    public ApiResult borrowBook(Borrow borrow) {
        Connection conn = connector.getConn();
        try {
            PreparedStatement stmt1 = conn.prepareStatement(   "SELECT stock FROM book WHERE book_id = ?");
            stmt1.setInt(1, borrow.getBookId());
            ResultSet rs1 = stmt1.executeQuery();
            if (!rs1.next() || rs1.getInt("stock") == 0) {
                return new ApiResult (false,"Book is not available.");
            }

            PreparedStatement stmt2 = conn.prepareStatement("SELECT * FROM borrow WHERE book_id = ? AND card_id = ? and return_time = 0");
            stmt2.setInt(1, borrow.getBookId());
            stmt2.setInt(2, borrow.getCardId());
            ResultSet rs2 = stmt2.executeQuery();
           if (rs2.next()) {
                return new ApiResult(false, "Book is already borrowed.");
            }

            PreparedStatement stmt3 = conn.prepareStatement("INSERT INTO borrow (card_id, book_id, borrow_time) VALUES (?, ?, ?)");
            stmt3.setInt(1, borrow.getCardId());
            stmt3.setInt(2, borrow.getBookId());
            stmt3.setLong(3, borrow.getBorrowTime());
            stmt3.executeUpdate();

            PreparedStatement stmt4 = conn.prepareStatement(
                    "UPDATE `book` SET `stock` = `stock` - 1 WHERE `book_id` = ?");
            stmt4.setInt(1, borrow.getBookId());
            stmt4.executeUpdate();
            //check again stock
            PreparedStatement stmt5 = conn.prepareStatement(   "SELECT stock FROM book WHERE book_id = ?");
            stmt5.setInt(1, borrow.getBookId());
            ResultSet rs5 = stmt5.executeQuery();
            rs5.next();
            if (rs5.getInt("stock") < 0) {
                rollback(conn);
                return new ApiResult (false,"Book is not available.");
            }
        } catch (Exception e){
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        commit(conn);
        return new ApiResult(true, null);
    }

    @Override
    public ApiResult returnBook(Borrow borrow) {
        Connection conn = connector.getConn();
        try{
            PreparedStatement stmt = conn.prepareStatement("SELECT * FROM borrow WHERE card_id=? AND book_id=? and borrow_time=? and return_time=0");
            stmt.setInt(1, borrow.getCardId());
            stmt.setInt(2, borrow.getBookId());
            stmt.setLong(3,borrow.getBorrowTime());
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {//if not exist such borrow record
                return new ApiResult(false, "Book does not exist");
            }
            else{
                PreparedStatement stmt2 = conn.prepareStatement("UPDATE book SET stock=stock+1 WHERE book_id=?");
                stmt2.setInt(1, borrow.getBookId());
                stmt2.executeUpdate();
                PreparedStatement stmt3 = conn.prepareStatement("Update borrow SET return_time=? WHERE card_id=? AND book_id=? and return_time=0 and borrow_time<?");//选未还的第一个
                stmt3.setLong(1,borrow.getReturnTime());
                stmt3.setInt(2, borrow.getCardId());
                stmt3.setInt(3, borrow.getBookId());
                stmt3.setLong(4,borrow.getReturnTime());
                stmt3.executeUpdate();
                commit(conn);
                return new ApiResult(true, "return book successfully");
            }
        }catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult showBorrowHistory(int cardId) {
        Connection conn = connector.getConn();
        try{
            PreparedStatement stmt = conn.prepareStatement("SELECT * FROM borrow WHERE card_id=? order by borrow_time desc,book_id asc");//莫忘排序
            stmt.setInt(1, cardId);
            ResultSet rs = stmt.executeQuery();
            List<Item> borrowList = new ArrayList<Item>();
                while(rs.next()){//这里要先用next，第一个没用
                    Item item = new Item();
                    item.setCardId(rs.getInt("card_id"));
                    item.setBookId(rs.getInt("book_id"));
                    item.setBorrowTime(rs.getLong("borrow_time"));
                    item.setReturnTime(rs.getLong("return_time"));
                    PreparedStatement stmt1= conn.prepareStatement("select * from book where book_id=?");
                    stmt1.setInt(1, rs.getInt("book_id"));
                    ResultSet rs1=stmt1.executeQuery();
                    rs1.next();//这里也要next
                    item.setAuthor(rs1.getString("author"));
                    item.setCategory(rs1.getString("category"));
                    item.setPress(rs1.getString("press"));
                    item.setPrice(rs1.getDouble("price"));
                    item.setPublishYear(rs1.getInt("publish_year"));
                    item.setTitle(rs1.getString("title"));
                    borrowList.add(item);
                }
                return new ApiResult(true, "show borrow history successfully", new BorrowHistories(borrowList));
            }catch(Exception e){
                return new ApiResult(false, e.getMessage());
            }
        }
        

    @Override
    public ApiResult registerCard(Card card) {
        Connection conn = connector.getConn();
        try{
            PreparedStatement stmt = conn.prepareStatement("SELECT * FROM card WHERE name=? AND department=? and type=?");
            stmt.setString(1, card.getName());
            stmt.setString(2, card.getDepartment());
            stmt.setString(3, card.getType().getStr());
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {//if exist
                return new ApiResult(false, "Card already exists");
            }
            else{
                PreparedStatement stmt2 = conn.prepareStatement("INSERT INTO card (name, department, type) VALUES ( ?, ?, ?)",Statement.RETURN_GENERATED_KEYS);
                stmt2.setString(1, card.getName());
                stmt2.setString(2, card.getDepartment());
                stmt2.setString(3, card.getType().getStr());
                stmt2.executeUpdate();
                ResultSet rs2 = stmt2.getGeneratedKeys();
            if (rs2.next()) {
                card.setCardId(rs2.getInt(1));//返回得到的主键
            }
            commit(conn);
                return new ApiResult(true, "register card successfully");
            }
        }catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }
    @Override
    public ApiResult removeCard(int cardId) {
        Connection conn = connector.getConn();
        try{
            PreparedStatement stmt = conn.prepareStatement("SELECT * FROM card WHERE card_id=?");
            stmt.setInt(1, cardId);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {//if not exist
                return new ApiResult(false, "Card does not exist");
            }
            else{//exist
                PreparedStatement stmt1=conn.prepareStatement("select * from borrow where card_id=? and return_time=0");
                stmt1.setInt(1, cardId);
                ResultSet rs1=stmt1.executeQuery();
                if(rs1.next()){//exist books that hasnt returned
                    return new ApiResult(false,"Card has books that didn't returned");
                }
                //else
                PreparedStatement stmt2 = conn.prepareStatement("DELETE FROM card WHERE card_id=?");
                stmt2.setInt(1, cardId);
                stmt2.executeUpdate();
                commit(conn);
                return new ApiResult(true, "remove card successfully");
            }
        }catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult showCards() {
        Connection conn = connector.getConn();
        try{
            PreparedStatement stmt = conn.prepareStatement("SELECT * FROM card order by card_id asc");
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {//if not exist
                return new ApiResult(true, "Card does not exist");
            }
            else{
                List<Card> cardList = new ArrayList<Card>();
                do{
                    String type =rs.getString("type");//处理type
                    if(Objects.equals(type, "S")){
                        type="Student";
                    }
                    else{
                        type="Teacher";
                    }
                    Card card = new Card();
                    card.setCardId(rs.getInt("card_id"));
                    card.setName(rs.getString("name"));
                    card.setDepartment(rs.getString("department"));
                    Card.CardType t=CardType.valueOf(type);//
                    card.setType(t);
                    cardList.add(card);
                }while(rs.next());
                return new ApiResult(true, "show cards successfully", new CardList(cardList));
            }
        }catch(SQLException e){
            e.printStackTrace();
        }
        return new ApiResult(false,"function not completed");
    }

    @Override
    public ApiResult resetDatabase() {
        Connection conn = connector.getConn();
        try {
            Statement stmt = conn.createStatement();
            DBInitializer initializer = connector.getConf().getType().getDbInitializer();
            stmt.addBatch(initializer.sqlDropBorrow());
            stmt.addBatch(initializer.sqlDropBook());
            stmt.addBatch(initializer.sqlDropCard());
            stmt.addBatch(initializer.sqlCreateCard());
            stmt.addBatch(initializer.sqlCreateBook());
            stmt.addBatch(initializer.sqlCreateBorrow());
            stmt.executeBatch();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    private void rollback(Connection conn) {
        try {
            conn.rollback();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void commit(Connection conn) {
        try {
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
