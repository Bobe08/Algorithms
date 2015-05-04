/**
 * Created by Bence on 2015.04.12..
 */
import com.sun.org.apache.bcel.internal.generic.NEW;

import javax.management.Query;
import java.awt.geom.Point2D;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.Random;


public class Main {

    private static DbConnection dbcon=new DbConnection();
    //!Todo felhasználótól bekérni, hogy hány clustert szeretne
    private static int clusterNumber=5;

    public static void main(String[] args){
        //Make a connection instance
        try {
            dbcon.connect("hive","");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        //add User Defiened Function to hortonworks
        intialization();

        //!TODO konzol bekéri az adattáblát és a két attribútumot amin klaszterezni szeretnénk
        //Initialize the working dataset
        //DataSetMetaInformation currentDataset=new DataSetMetaInformation("default.flights_with_less_data",
          //      new String[]{"Distance", "AirTime"});

        DataSetMetaInformation currentDataset=new DataSetMetaInformation("default.testdata",
               new String[]{"x", "y"});

        //double[] centroids=new double[]{16700,192780};
        //double[] centroids=new double[]{36000,83000};
        //double[] centroids=new double[]{50,180};
        //centroids=k_mean_algorithm_flights(centroids);
        //centroids=k_mean_algorithm(centroids);
        //simpleQuery();

        //!TODO Megkeresni azokat azt a k pontot ahhonnan a klaszterezést indítjuk
        //Initialize starting centroids
        Point2D.Double[] centroids=range_method(currentDataset,clusterNumber);

        for(Point2D.Double centroid:centroids){
            System.out.println(centroid.getX()+"--"+centroid.getY());
        }


        Random rand=new Random();
        for (int i = 0; i < 1; i++) {


            Point2D.Double[] centroids2 = new Point2D.Double[]{new Point2D.Double(rand.nextInt(50)+50, rand.nextInt(50)+350)
                                                               ,new Point2D.Double(rand.nextInt(50)+260, rand.nextInt(50)+1700)
                                                               ,new Point2D.Double(rand.nextInt(50)+130, rand.nextInt(50)+900)
                                                               };
            Point2D.Double[] centroids3 = new Point2D.Double[]{new Point2D.Double(32, 250)
                    ,new Point2D.Double(222, 1200)
                    ,new Point2D.Double(310, 1900)
                    ,new Point2D.Double(150, 800)
                    ,new Point2D.Double(80, 500)
            };


            Point2D.Double[] oldcentroids = centroidCopy(centroids);

            //The duration of run
            long time;

            //Starting the timer
            long starTime = System.nanoTime();




            //Finding the centroids
            centroids = find_centroids_k_mean_algorithm_in_2d_faster(centroids, currentDataset);

            time = (System.nanoTime() - starTime) / 1000000000;
            Write_to_csv_file("run_information.csv", oldcentroids, centroids, time);

            //Writing all the data points and the cluster index to a csv file
            ClusterPoints(centroids,currentDataset);


            System.out.println();
            for (int j = 0; j < centroids.length; j++) {
                System.out.println("x: "+centroids[j].getX()+"-- y: "+centroids[j].getY());
            }
        }
    }

    /**
     * Init the starting centroids
     * @param currentDataset:
     *            name:the table name, what the program can use after the from statement
     *            attributes:[0] and [1] are the attributes that, the algorithm use to do 2d clustering
     * @param clusterNumber: The number of clusters
     * @return
     */
    private static Point2D.Double[] range_method(DataSetMetaInformation currentDataset, int clusterNumber) {

        Statement stmt = null;
        Point2D.Double[] temp= new Point2D.Double[clusterNumber];
        Point2D.Double min=new Point2D.Double();
        Point2D.Double max=new Point2D.Double();
        try {
            //Query the minimum, maximum and average of the datapoints
            stmt = dbcon.connection.createStatement();
            String query="select min(cast("+currentDataset.attributes[0]+" as double)),min(cast("+currentDataset.attributes[1]+" as double))," +
                    "max(cast("+currentDataset.attributes[0]+" as double)),max(cast("+currentDataset.attributes[1]+" as double)) " +
                    //"avg("+currentDataset.attributes[0]+"),avg("+currentDataset.attributes[1]+") " +
                    "from "+currentDataset.name;
            System.out.println(query);
            ResultSet rset = stmt.executeQuery(query);





            if(rset.next()){
                //setting the minimum and maximum
                min.setLocation(rset.getDouble(1),rset.getDouble(2));
                max.setLocation(rset.getDouble(3),rset.getDouble(4));
            }

         System.out.println("min: "+min.getX()+"--"+min.getY());
            System.out.println("max: "+max.getX()+"--"+max.getY());

        } catch (SQLException e) {
            e.printStackTrace();
        }

        Point2D.Double difference= new Point2D.Double(max.getX()-min.getX(),max.getY()-min.getY());
        for (int i = 0; i < clusterNumber; i++) {
            //Init the starting centroids
            temp[i]=new Point2D.Double(min.getX()+(difference.getX()/clusterNumber)*i,min.getY()+(difference.getY()/clusterNumber)*i);
        }
        return temp;
    }

    private static void intialization() {
        Statement stmt = null;
        try {
            stmt =dbcon.connection.createStatement();
            stmt.execute("add jar /usr/tmp/HiveSwarm-1.0.jar");



            stmt.execute("create temporary function least as 'com.livingsocial.hive.udf.GenericUDFLeast'");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /**
     * Writing all the data points to a csv file
     * @param centroids: the data point get a centroid index number, always to the nearest centroid.
     */
    private static void ClusterPoints(Point2D.Double[] centroids, DataSetMetaInformation dsmi) {
        //Making the query string
        //The first 2 attributes is the centroids
        String queryString="select "+dsmi.attributes[0]+","+dsmi.attributes[1]+",(";
        queryString+=create_choose_least_statement(centroids.length, dsmi);

        queryString+=") as order from "+dsmi.name+" ";
        queryString+="order by order";


        try {
            //Making the sql statement
            PreparedStatement stmt = dbcon.connection.prepareStatement(queryString);
            int count=1;
            //Setting the parameters for PreparedStatement (Setting the parameters to Select ... case when...)
            for(int i=0;i<centroids.length;i++) {
                stmt.setDouble(count++,centroids[i].getX());
                stmt.setDouble(count++,centroids[i].getY());
            }
            for(int i=0;i<centroids.length;i++) {
                stmt.setDouble(count++,centroids[i].getX());
                stmt.setDouble(count++,centroids[i].getY());
            }



            //Execute query
            ResultSet rs = stmt.executeQuery();


            //The csv Writer
            final String COMMA = ",";
            final String NEW_LINE_SEPARATOR = "\n";
            FileWriter fileWriter=null;
            //!TODO a felhasználótól kérje be a mentés helyét
            String fileName="result_data.csv";
            try {
                //true: overwrite the file not append
                fileWriter= new FileWriter(fileName,false);

                //the number of clusters (k)
                fileWriter.append("clusterNumber,"+centroids.length);
                fileWriter.append(NEW_LINE_SEPARATOR);

                //header
                fileWriter.append("id," + dsmi.attributes[0]+","+dsmi.attributes[1]+",centroidID");

                fileWriter.append(NEW_LINE_SEPARATOR);
                //Write the data to the file
                int id=0;
                while (rs.next()){
                    try {
                        rs.getDouble(1);
                        rs.getDouble(2);
                        rs.getInt(3);

                        fileWriter.append(String.valueOf(id++));
                        fileWriter.append(COMMA);
                        fileWriter.append(String.valueOf(rs.getDouble(1)));
                        fileWriter.append(COMMA);
                        fileWriter.append(String.valueOf(rs.getDouble(2)));
                        fileWriter.append(COMMA);
                        fileWriter.append(String.valueOf(rs.getInt(3)));

                        fileWriter.append(NEW_LINE_SEPARATOR);
                    }catch (SQLException e){}
                }


            } catch (Exception e) {

                e.printStackTrace();
            } finally {

                try {
                    fileWriter.flush();
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void Write_to_csv_file(String fileName, Point2D.Double[] oldcentroids,
                                          Point2D.Double[] newcentroids, long time) {
        //Delimiter used in CSV file
        final String COMMA = ",";
        final String NEW_LINE_SEPARATOR = "\n";
        FileWriter fileWriter=null;
        try {
            //true: to append the file not to overwrite
            fileWriter= new FileWriter(fileName,true);


            //Write the oldcentroids to a csv file
            for (Point2D.Double point: oldcentroids) {
                fileWriter.append(String.valueOf(point.getX()));
                fileWriter.append(COMMA);
                fileWriter.append(String.valueOf(point.getY()));
                fileWriter.append(COMMA);
            }

            //Write the newcentroids to a csv file
            for (Point2D.Double point: newcentroids) {
                fileWriter.append(String.valueOf(point.getX()));
                fileWriter.append(COMMA);
                fileWriter.append(String.valueOf(point.getY()));
                fileWriter.append(COMMA);
            }

            fileWriter.append(String.valueOf(time));
            fileWriter.append(NEW_LINE_SEPARATOR);


        } catch (Exception e) {

            e.printStackTrace();
        } finally {

            try {
                fileWriter.flush();
                fileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }


    /**
     *The faster algorithm
     * @param centroids: the given centroids for k-mean algorithm
     * @param dsmi:
     *            name:the table name, what the program can use after the from statement
     *            attributes:[0] and [1] are the attributes that, the algorithm use to do 2d clustering
     * @return returns with the real 2d centroids
     */
    public static Point2D.Double[] find_centroids_k_mean_algorithm_in_2d_faster(Point2D.Double[] centroids, DataSetMetaInformation dsmi){
        //Making a temp array
        Point2D.Double[] centroidsTemp=new Point2D.Double[centroids.length];
        //Copying the centroidsTemp to centroids
        centroidsTemp=centroidCopy(centroids);

        //Making the query string
        //The first 2 attributes is the centroids
        String queryString="select avg("+dsmi.attributes[0]+"),avg("+dsmi.attributes[1]+"),(";
        queryString+=create_choose_least_statement(centroids.length, dsmi)+" ";

        queryString+=") as order from "+dsmi.name+" group by ";
        queryString+=create_choose_least_statement(centroids.length, dsmi);

        queryString+="order by order";


        //Making the sql statement
        PreparedStatement stmt = null;
        try {
            stmt = dbcon.connection.prepareStatement(queryString);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(queryString);
        boolean run = true;
        while (run) {
            try {




                int count=1;
                //Setting the parameters for PreparedStatement (Setting the parameters to Select ... case when...)
                for (int i = 0; i < centroids.length; i++) {
                    stmt.setDouble(count++,centroids[i].getX());
                    stmt.setDouble(count++,centroids[i].getY());
                }
                for (int i = 0; i < centroids.length; i++) {
                    stmt.setDouble(count++,centroids[i].getX());
                    stmt.setDouble(count++,centroids[i].getY());
                }

                //(Setting the parameters to Group by case when...)
                for (int i = 0; i < centroids.length; i++) {
                    stmt.setDouble(count++,centroids[i].getX());
                    stmt.setDouble(count++,centroids[i].getY());
                }
                for (int i = 0; i < centroids.length; i++) {
                    stmt.setDouble(count++,centroids[i].getX());
                    stmt.setDouble(count++,centroids[i].getY());
                }


                //Execute query
                ResultSet rs = stmt.executeQuery();
                int i = 0;
                while (rs.next()) {
                        centroidsTemp[i].setLocation(rs.getDouble(1), rs.getDouble(2));

                        System.out.println(i + ". average x: " + centroidsTemp[i].getX());
                        System.out.println(i + ". average y: " + centroidsTemp[i].getY());
                        i++;
                }

                //!Todo Egy számot hozzáadni amiután biztosan leáll az algoritmus
                //Comparing the centroids and centroidsTemp, if it is equals, the algorithm is ready
                for (int j=0;j<centroids.length;j++){
                    if(centroids[j].getX()!=centroidsTemp[j].getX() || centroids[j].getY()!=centroidsTemp[j].getY())
                        break;
                    if(j==(centroids.length-1))
                        run=false;
                }

                if(!run){
                    break;
                }

                //Copying the centroidsTemp to centroids
                centroids=centroidCopy(centroidsTemp);


            }catch(SQLException e){
                e.printStackTrace();
            }

        }
        return centroids;
    }

    private static String create_choose_least_statement(int length, DataSetMetaInformation dsmi) {
        String queryString="case least(";
        for (int i = 0; i < length; i++) {
            queryString+="sqrt(pow(cast("+dsmi.attributes[0]+ " as double)-?,2)+pow(cast("+dsmi.attributes[1]+ " as double)-?,2))";
            if(i<length-1)
                queryString+=",";
        }
        queryString+=") ";
        for (int i = 0; i < length; i++) {
            queryString+="when sqrt(pow(cast("+dsmi.attributes[0]+ " as double)-?,2)+pow(cast("+dsmi.attributes[1]+ " as double)-?,2)) then "+i+" ";
        }
        queryString+="end ";
        return queryString;
    }

    /**
     *
     * @param centroids: the given centroids for k-mean algorithm
     * @param dsmi:
     *            name:the table name, what the program can use after the from statement
     *            attributes:the right attributes name
     * @return returns with the real 2d centroids
     */
    public static Point2D.Double[] find_centroids_k_mean_algorithm_in_2d(Point2D.Double[] centroids, DataSetMetaInformation dsmi){
        //Making a temp array
        Point2D.Double[] centroidsTemp=new Point2D.Double[centroids.length];
        //Copying the centroidsTemp to centroids
        centroidsTemp=centroidCopy(centroids);

        //Making the query string
        //The first 2 attributes is the centroids
        String queryString="select avg("+dsmi.attributes[0]+"),avg("+dsmi.attributes[1]+"),(";
        queryString+=create_case_when_statement(centroids.length,dsmi);

        queryString+=") as order from "+dsmi.name+" group by ";
        queryString+=create_case_when_statement(centroids.length,dsmi);
        queryString+="order by order";


        //Making the sql statement
        PreparedStatement stmt = null;
        boolean run = true;
        while (run) {
            try {
                System.out.println(queryString);
                stmt = dbcon.connection.prepareStatement(queryString);


                int count=1;
                //Setting the parameters for PreparedStatement (Setting the parameters to Select ... case when...)
                for (int i = 0; i < centroids.length-1; i++) {
                    for(int j=i+1;j<centroids.length;j++) {
                        stmt.setDouble(count++,centroids[i].getX());
                        stmt.setDouble(count++,centroids[j].getX());
                    }
                }

                //(Setting the parameters to Group by case when...)
                for (int i = 0; i < centroids.length-1; i++) {
                    for(int j=i+1;j<centroids.length;j++) {
                        stmt.setDouble(count++,centroids[i].getX());
                        stmt.setDouble(count++,centroids[j].getX());
                    }
                }


                //Execute query
                ResultSet rs = stmt.executeQuery();
                double[] distance=new double[centroids.length];
                int i = 0;
                while (rs.next()) {
                    centroidsTemp[i].setLocation(rs.getDouble(1),rs.getDouble(2));
                    distance[i]=rs.getDouble(2);

                    System.out.println(i+": "+centroidsTemp[i]);
                    System.out.println(i+". average distance(miles): "+distance[i]);
                    i++;
                }

                //Comparing the centroids and centroidsTemp, if it is equals, the algorithm is ready
                for (int j=0;j<centroids.length;j++){
                    if(centroids[j].getX()!=centroidsTemp[j].getX() || centroids[j].getY()!=centroidsTemp[j].getY())
                        break;
                    if(j==(centroids.length-1))
                        run=false;
                }

                if(!run){
                    break;
                }

                //Copying the centroidsTemp to centroids
                centroids=centroidCopy(centroidsTemp);


            }catch(SQLException e){
                e.printStackTrace();
            }

        }
        return centroids;
    }

    /**
     *
     * @param length: the number of centroids
     * @param dsmi: the DataSetMetaInformation
     * @return: the final case when.. string
     */
    private static String create_case_when_statement(int length,DataSetMetaInformation dsmi) {
        String queryString="case ";
        for (int i = 0; i < length-1; i++) {
            queryString+="when ";
            for(int j=i+1;j<length;j++) {
                queryString+="abs("+dsmi.attributes[0]+"-?)<=abs("+ dsmi.attributes[0]+"-?) ";
                if(j!=length-1)
                    queryString+="and ";
            }
            queryString+="then "+i+" ";
        }
        queryString+="else "+(length-1)+" end ";
        return queryString;
    }



    /**
     * Copying the centroidsTemp to centroids
     * @param centroids: the centroids to copy
     * @return: the copied centroids array
     */
    public static Point2D.Double[] centroidCopy(Point2D.Double[] centroids){
        Point2D.Double[] centroidsTemp=new Point2D.Double[centroids.length];
        for (int k = 0; k < centroids.length; k++) {
            centroidsTemp[k]=new Point2D.Double(centroids[k].getX(),centroids[k].getY());
        }
        return centroidsTemp;
    }

    //centroids: the given centroids for k-mean algorithm
    public static double[] k_mean_algorithm(double[] centroids){
        //Copying the input array
        double[] centroidsTemp=new double[centroids.length];
        System.arraycopy(centroids,0,centroidsTemp,0,centroids.length);

        //Making the sql statement
        PreparedStatement stmt = null;
        boolean run = true;
        while (run) {
            try {
                stmt = dbcon.connection.prepareStatement("select avg(salary), count(case when abs(salary-?)<abs(salary-?) then 0 else 1 end)\n" +
                        "from default.sample_07\n" +
                        "group by case\n" +
                        "when abs(salary-?)<abs(salary-?) then 0\n" +
                        "else 1 end");


                //Setting the parameters for PreparedStatement
                stmt.setDouble(1, centroids[0]);
                stmt.setDouble(2, centroids[1]);
                stmt.setDouble(3, centroids[0]);
                stmt.setDouble(4, centroids[1]);

                //Execute query
                ResultSet rs = stmt.executeQuery();
                int[] count=new int[centroids.length];
                int i = 0;
                while (rs.next()) {
                    centroidsTemp[i] = rs.getDouble(1);
                    count[i]=rs.getInt(2);

                    System.out.println(i+": "+centroidsTemp[i]);
                    System.out.println(i+". element count: "+count[i]);
                    i++;
                }

                //Comparing the centroids and centroidsTemp, if it is equals, the algorithm is ready
                for (int j=0;j<centroids.length;j++){
                    if(centroids[j]!=centroidsTemp[j])
                        break;
                    if(j==(centroids.length-1))
                        run=false;
                }

                if(!run){
                    break;
                }

                //Copying the centroidsTemp to centroids
                System.arraycopy(centroidsTemp,0,centroids,0,centroids.length);

            }catch(SQLException e){
                e.printStackTrace();
            }

        }
        return centroids;
    }

    //centroids: the given centroids for k-mean algorithm
    public static double[] k_mean_algorithm_flights(double[] centroids){
        //Copying the input array
        double[] centroidsTemp=new double[centroids.length];
        System.arraycopy(centroids,0,centroidsTemp,0,centroids.length);

        //Making the sql statement
        PreparedStatement stmt = null;
        boolean run = true;
        while (run) {
            try {
                stmt = dbcon.connection.prepareStatement("select avg(AirTime),avg(Distance), count(case when abs(AirTime-?)<abs(AirTime-?) then 0 else 1 end)\n" +
                        "from default.flights\n" +
                        "group by case\n" +
                        "when abs(AirTime-?)<abs(AirTime-?) then 0\n" +
                        "else 1 end");


                //Setting the parameters for PreparedStatement
                stmt.setDouble(1, centroids[0]);
                stmt.setDouble(2, centroids[1]);
                stmt.setDouble(3, centroids[0]);
                stmt.setDouble(4, centroids[1]);

                //Execute query
                ResultSet rs = stmt.executeQuery();
                double[] distance=new double[centroids.length];
                int i = 0;
                while (rs.next()) {
                    centroidsTemp[i] = rs.getDouble(1);
                    distance[i]=rs.getDouble(2);

                    System.out.println(i+": "+centroidsTemp[i]);
                    System.out.println(i+". average distance(miles): "+distance[i]);
                    System.out.println(i+". element count: "+rs.getInt(3));
                    i++;
                }

                //Comparing the centroids and centroidsTemp, if it is equals, the algorithm is ready
                for (int j=0;j<centroids.length;j++){
                    if(centroids[j]!=centroidsTemp[j])
                        break;
                    if(j==(centroids.length-1))
                        run=false;
                }

                if(!run){
                    break;
                }

                //Copying the centroidsTemp to centroids
                System.arraycopy(centroidsTemp,0,centroids,0,centroids.length);

            }catch(SQLException e){
                e.printStackTrace();
            }

        }
        return centroids;
    }


    public static void simpleQuery(){
        Statement stmt = null;
        try {
            stmt = dbcon.connection.createStatement();
            ResultSet rset = stmt.executeQuery("select avg(AirTime),avg(Distance) from default.flights_with_less_data group by least(AirTime, Distance) limit 50");

            int i=0;
            while(rset.next()){
                System.out.println(i+": "+rset.getDouble(1)+"--"+rset.getDouble(2)+"--"+rset.getDouble(2));
                i++;
            }


        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}