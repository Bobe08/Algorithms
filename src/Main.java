/**
 * Created by Bence on 2015.04.12..
 */
import com.sun.org.apache.bcel.internal.generic.NEW;

import javax.management.Query;
import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Random;


public class Main {



    private static DbConnection dbcon = new DbConnection();

    private static int clusterNumber = 3;

    //Where the result saved:
    private static String result_place="result_data.csv";

    public static void main(String[] args) {
        //Make a connection instance
        try {
            dbcon.connect("hive", "");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        //add User Defiened Function to hortonworks
        intialization();


        //Reading the tables and attributes names
        String tableName = "default.flights_with_less_data";
        String attributes[] = {"Distance","AirTime"};
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("Enter the dataset name (e.g.: default.testdata):\n (Or hit enter and choose the default dataset) : ");
        String nameTemp = "";
        try {
            nameTemp = br.readLine();
            if (nameTemp.isEmpty())
                nameTemp = tableName;
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.print("Enter the first attribute name (x coordinate):\n (Or hit enter and choose the default one) : ");
        String attributeTemp1 = "";
        try {
            attributeTemp1 = br.readLine();
            if (attributeTemp1.isEmpty())
                attributeTemp1 = attributes[0];
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.print("Enter the second attribute name (y coordinate):\n (Or hit enter and choose the default one) : ");
        String attributeTemp2 = "";
        try {
            attributeTemp2 = br.readLine();
            if (attributeTemp2.isEmpty())
                attributeTemp2 = attributes[1];
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            simpleQuery(nameTemp, new String[]{attributeTemp1, attributeTemp2});
            tableName = nameTemp;
            attributes[0] = attributeTemp1;
            attributes[1] = attributeTemp2;
        } catch (SQLException e) {
            System.out.println("Wrong table or attributes name!");
        }


        //Initialize the working dataset
        DataSetMetaInformation currentDataset = new DataSetMetaInformation(tableName, attributes);


        //Where the result save
        System.out.print("Where would you like to save the result (___.csv)? (e.g.: save_data.csv)");
        try {
            String line=br.readLine();
            if (!line.isEmpty() && line.matches("^\\w+\\.csv$"))
               result_place =line;
            else
                System.out.print("Wrong file name, the result will be saved in: ");
            System.out.println(result_place);
        } catch (IOException e) {
            e.printStackTrace();
        }


        //Getting the cluster number
        System.out.print("How many clusters would you like? ");
        try {
            String line=br.readLine();
            if (!line.isEmpty() && line.matches("^\\d+$"))
                clusterNumber = Integer.parseInt(line);
            else
                System.out.print("Wrong input!");
        } catch (IOException e) {
            e.printStackTrace();
        }



        //Initialize starting centroids
        Point2D.Double[] centroids = range_method(currentDataset, clusterNumber);

        for (Point2D.Double centroid : centroids) {
            System.out.println(centroid.getX() + "--" + centroid.getY());
        }


        Random rand = new Random();
        for (int i = 0; i < 1; i++) {


            Point2D.Double[] centroids2 = new Point2D.Double[]{new Point2D.Double(rand.nextInt(50) + 50, rand.nextInt(50) + 350)
                    , new Point2D.Double(rand.nextInt(50) + 260, rand.nextInt(50) + 1700)
                    , new Point2D.Double(rand.nextInt(50) + 130, rand.nextInt(50) + 900)
            };
            Point2D.Double[] centroids3 = new Point2D.Double[]{new Point2D.Double(32, 250)
                    , new Point2D.Double(222, 1200)
                    , new Point2D.Double(310, 1900)
                    , new Point2D.Double(150, 800)
                    , new Point2D.Double(80, 500)
            };


            Point2D.Double[] oldcentroids = centroidCopy(centroids);

            //The duration of run
            long time;

            //Starting the timer
            long starTime = System.nanoTime();


            //Finding the centroids
            centroids = find_centroids_k_mean_algorithm_in_2d_faster(centroids, currentDataset);


            //Calculating the runtime in seconds
            time = (System.nanoTime() - starTime) / 1000000000;
            Write_to_csv_file("run_information.csv",currentDataset, oldcentroids, centroids, time);

            //Writing all the data points and the cluster index to a csv file
            ClusterPoints(centroids, currentDataset);


            System.out.println();
            for (int j = 0; j < centroids.length; j++) {
                System.out.println("x: " + centroids[j].getX() + "-- y: " + centroids[j].getY());
            }
        }
    }

    /**
     * Init the starting centroids
     *
     * @param currentDataset: name:the table name, what the program can use after the from statement
     *                        attributes:[0] and [1] are the attributes that, the algorithm use to do 2d clustering
     * @param clusterNumber:  The number of clusters
     * @return
     */
    private static Point2D.Double[] range_method(DataSetMetaInformation currentDataset, int clusterNumber) {

        Statement stmt = null;
        Point2D.Double[] temp = new Point2D.Double[clusterNumber];
        Point2D.Double min = new Point2D.Double();
        Point2D.Double avg = new Point2D.Double();
        Point2D.Double max = new Point2D.Double();
        try {
            //Query the minimum, maximum and average of the datapoints
            stmt = dbcon.connection.createStatement();
            String query = "select min(cast(" + currentDataset.attributes[0] + " as double)),min(cast(" + currentDataset.attributes[1] + " as double))," +
                    "max(cast(" + currentDataset.attributes[0] + " as double)),max(cast(" + currentDataset.attributes[1] + " as double))," +
                    "avg("+currentDataset.attributes[0]+"),avg("+currentDataset.attributes[1]+") " +
                    "from " + currentDataset.name;
            System.out.println(query);
            ResultSet rset = stmt.executeQuery(query);


            if (rset.next()) {
                //setting the minimum and maximum
                min.setLocation(rset.getDouble(1), rset.getDouble(2));
                max.setLocation(rset.getDouble(3), rset.getDouble(4));
                avg.setLocation(rset.getDouble(5), rset.getDouble(6));
            }

            System.out.println("min: " + min.getX() + "--" + min.getY());
            System.out.println("avg: " + avg.getX() + "--" + avg.getY());
            System.out.println("max: " + max.getX() + "--" + max.getY());

        } catch (SQLException e) {
            e.printStackTrace();
        }

        //the difference is the smallest distance twice from the average (2*(avg-min) or 2*(max-avg))
        Point2D.Double difference = new Point2D.Double((max.getX()-avg.getX())<(avg.getX()-min.getX())? 2*(max.getX()-avg.getX()):2*(avg.getX()-min.getX())
                                                        ,(max.getY()-avg.getY())<(avg.getY()-min.getY())? 2*(max.getY()-avg.getY()):2*(avg.getY()-min.getY()));

        System.out.println("difference: " + difference.getX() + "--" + difference.getY());

        for (int i = 0; i < clusterNumber; i++) {
            //Init the starting centroids
            temp[i] = new Point2D.Double(min.getX() + (difference.getX() / clusterNumber) * i, min.getY() + (difference.getY() / clusterNumber) * i);
        }
        return temp;
    }

    private static void intialization() {
        Statement stmt = null;
        try {
            stmt = dbcon.connection.createStatement();
            stmt.execute("add jar /usr/tmp/HiveSwarm-1.0.jar");


            stmt.execute("create temporary function least as 'com.livingsocial.hive.udf.GenericUDFLeast'");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /**
     * Writing all the data points to a csv file
     *
     * @param centroids: the data point get a centroid index number, always to the nearest centroid.
     */
    private static void ClusterPoints(Point2D.Double[] centroids, DataSetMetaInformation dsmi) {
        //Making the query string
        //The first 2 attributes is the centroids
        String queryString = "select " + dsmi.attributes[0] + "," + dsmi.attributes[1] + ",(";
        queryString += create_choose_least_statement(centroids.length, dsmi);

        queryString += ") as order from " + dsmi.name + " ";
        //queryString += "order by order";


        try {
            //Making the sql statement
            PreparedStatement stmt = dbcon.connection.prepareStatement(queryString);
            int count = 1;
            //Setting the parameters for PreparedStatement (Setting the parameters to Select ... case when...)
            for (int i = 0; i < centroids.length; i++) {
                stmt.setDouble(count++, centroids[i].getX());
                stmt.setDouble(count++, centroids[i].getY());
            }
            for (int i = 0; i < centroids.length; i++) {
                stmt.setDouble(count++, centroids[i].getX());
                stmt.setDouble(count++, centroids[i].getY());
            }


            //Execute query
            ResultSet rs = stmt.executeQuery();


            //The csv Writer
            final String COMMA = ",";
            final String NEW_LINE_SEPARATOR = "\n";
            FileWriter fileWriter = null;

            try {
                //true: overwrite the file not append
                fileWriter = new FileWriter(result_place, false);

                //the number of clusters (k)
                fileWriter.append("clusterNumber," + centroids.length);
                fileWriter.append(NEW_LINE_SEPARATOR);

                //header
                fileWriter.append("id," + dsmi.attributes[0] + "," + dsmi.attributes[1] + ",centroidID");

                fileWriter.append(NEW_LINE_SEPARATOR);
                //Write the data to the file
                int id = 0;
                while (rs.next()) {
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
                    } catch (SQLException e) {
                    }
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

    private static void Write_to_csv_file(String fileName,DataSetMetaInformation dsmi, Point2D.Double[] oldcentroids,
                                          Point2D.Double[] newcentroids, long time) {
        //Delimiter used in CSV file
        final String COMMA = ",";
        final String NEW_LINE_SEPARATOR = "\n";
        FileWriter fileWriter = null;
        try {
            //true: to append the file not to overwrite
            fileWriter = new FileWriter(fileName, true);

            fileWriter.append(dsmi.name);
            fileWriter.append(COMMA);

            fileWriter.append(String.valueOf(time));
            fileWriter.append(COMMA);

            fileWriter.append(String.valueOf(oldcentroids.length));
            fileWriter.append(COMMA);

            fileWriter.append(result_place);
            fileWriter.append(COMMA);


            //Write the oldcentroids to a csv file
            for (Point2D.Double point : oldcentroids) {
                fileWriter.append(String.valueOf(point.getX()));
                fileWriter.append(COMMA);
                fileWriter.append(String.valueOf(point.getY()));
                fileWriter.append(COMMA);
            }

            //Write the newcentroids to a csv file
            for (Point2D.Double point : newcentroids) {
                fileWriter.append(String.valueOf(point.getX()));
                fileWriter.append(COMMA);
                fileWriter.append(String.valueOf(point.getY()));
                if(point!=newcentroids[newcentroids.length-1])
                    fileWriter.append(COMMA);

            }
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
     * The faster algorithm
     *
     * @param centroids: the given centroids for k-mean algorithm
     * @param dsmi:      name:the table name, what the program can use after the from statement
     *                   attributes:[0] and [1] are the attributes that, the algorithm use to do 2d clustering
     * @return returns with the real 2d centroids
     */
    public static Point2D.Double[] find_centroids_k_mean_algorithm_in_2d_faster(Point2D.Double[] centroids, DataSetMetaInformation dsmi) {
        //Making a temp array
        Point2D.Double[] centroidsTemp = new Point2D.Double[centroids.length];
        //Copying the centroidsTemp to centroids
        centroidsTemp = centroidCopy(centroids);

        //Making the query string
        //The first 2 attributes is the centroids
        String queryString = "select avg(" + dsmi.attributes[0] + "),avg(" + dsmi.attributes[1] + "),(";
        queryString += create_choose_least_statement(centroids.length, dsmi) + " ";

        queryString += ") as order from " + dsmi.name + " group by ";
        queryString += create_choose_least_statement(centroids.length, dsmi);

        queryString += "order by order";


        //Making the sql statement
        PreparedStatement stmt = null;
        try {
            stmt = dbcon.connection.prepareStatement(queryString);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(queryString);
        boolean run = true;

        //if the centroid changes less than the minimum change or steps more then the maximum step, the algorithm stops
        double minChange=0.01;
        int maxSteps=40;

        //how many iteration the algorithm have
        int steps=0;

        while (run) {
            try {


                int count = 1;
                //Setting the parameters for PreparedStatement (Setting the parameters to Select ... case when...)
                for (int i = 0; i < centroids.length; i++) {
                    stmt.setDouble(count++, centroids[i].getX());
                    stmt.setDouble(count++, centroids[i].getY());
                }
                for (int i = 0; i < centroids.length; i++) {
                    stmt.setDouble(count++, centroids[i].getX());
                    stmt.setDouble(count++, centroids[i].getY());
                }

                //(Setting the parameters to Group by case when...)
                for (int i = 0; i < centroids.length; i++) {
                    stmt.setDouble(count++, centroids[i].getX());
                    stmt.setDouble(count++, centroids[i].getY());
                }
                for (int i = 0; i < centroids.length; i++) {
                    stmt.setDouble(count++, centroids[i].getX());
                    stmt.setDouble(count++, centroids[i].getY());
                }


                //Execute query
                ResultSet rs = stmt.executeQuery();
                int i = 0;
                while (rs.next()) {
                    if(rs.getString(1)!=null && rs.getString(2)!=null) {
                        centroidsTemp[i].setLocation(rs.getDouble(1), rs.getDouble(2));

                        System.out.println(i + ". average x: " + centroidsTemp[i].getX()+" difference: "+(centroidsTemp[i].getX()-centroids[i].getX()));
                        System.out.println(i + ". average y: " + centroidsTemp[i].getY()+" difference: "+(centroidsTemp[i].getY()-centroids[i].getY()));
                        i++;
                    }
                }




                //Comparing the centroids and centroidsTemp, if it is equals or the change is very little the algorithm is ready
                for (int j = 0; j < centroids.length; j++) {
                    if (Math.abs(centroids[j].getX() - centroidsTemp[j].getX())>minChange  || Math.abs(centroids[j].getY() - centroidsTemp[j].getY())>minChange)
                        break;
                    if (j == (centroids.length - 1))
                        run = false;
                }

                //if the algorithm have more iteration then maxStep, then the while cycle stops
                if(maxSteps<steps){
                    run=false;
                    break;
                }
                System.out.println("Step: "+steps);

                if (!run) {
                    break;
                }

                //Copying the centroidsTemp to centroids
                centroids = centroidCopy(centroidsTemp);


            } catch (SQLException e) {
                e.printStackTrace();
            }

            //one more step
            steps++;
        }
        return centroids;
    }

    private static String create_choose_least_statement(int length, DataSetMetaInformation dsmi) {
        String queryString = "case least(";
        for (int i = 0; i < length; i++) {
            queryString += "sqrt(pow(cast(" + dsmi.attributes[0] + " as double)-?,2)+pow(cast(" + dsmi.attributes[1] + " as double)-?,2))";
            if (i < length - 1)
                queryString += ",";
        }
        queryString += ") ";
        for (int i = 0; i < length; i++) {
            queryString += "when sqrt(pow(cast(" + dsmi.attributes[0] + " as double)-?,2)+pow(cast(" + dsmi.attributes[1] + " as double)-?,2)) then " + i + " ";
        }
        queryString += "end ";
        return queryString;
    }



    /**
     * Copying the centroidsTemp to centroids
     *
     * @param centroids: the centroids to copy
     * @return: the copied centroids array
     */
    public static Point2D.Double[] centroidCopy(Point2D.Double[] centroids) {
        Point2D.Double[] centroidsTemp = new Point2D.Double[centroids.length];
        for (int k = 0; k < centroids.length; k++) {
            centroidsTemp[k] = new Point2D.Double(centroids[k].getX(), centroids[k].getY());
        }
        return centroidsTemp;
    }




    public static void simpleQuery(String name, String[] attributes) throws SQLException {
        Statement stmt = null;
        stmt = dbcon.connection.createStatement();
        ResultSet rset = stmt.executeQuery("select " + attributes[0] + "," + attributes[1] + " from " + name + " limit 1");

    }
}