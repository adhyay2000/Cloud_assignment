package com.project.contracts;

import com.project.utils.Tables;

/**
 * Class that handles returning of the index of a required column from a given table
 * and returning the csv file associated with a given table
 */
public class DBManager implements Cloneable {

    private DBManager() {
        // private constructor to restrict object creation
    }

    /**
     * Method that returns the index of a given column
     *
     * @param table  The table in which the column is present
     * @param column The column whose index is required
     * @return index of the column
     * @throws IllegalArgumentException when either the table or the column are invalid
     */
    public static int getColumnIndex(Tables table, String column)
            throws IllegalArgumentException {
        //System.out.println(">>>>>>" + table.name() + "." + column);
        switch (table) {
            case PRODUCTS:
                return ProductsContracts.getColumnIndex(column);
            // case SIMILAR:
            //     return SimilarContracts.getColumnIndex(column);
            // case CATEGORIES:
            //     return CategoriesContract.getColumnIndex(column);
            case REVIEWS:
                return ReviewsContract.getColumnIndex(column);
            default:
                throw new IllegalArgumentException("Table " + table.name().toLowerCase() + " does not exist");
        }
    }

    /**
     * Method that returns column name from given index
     *
     * @param table The table in which the column is present
     * @param index The index whos corresponding name is required
     * @return Column name
     * @throws IllegalArgumentException when either the table or the column index are invalid
     */
    public static String getColumnFromIndex(Tables table, int index)
            throws IllegalArgumentException {
        switch (table) {
            case PRODUCTS:
                return ProductsContracts.getColumnFromIndex(index);
            // case SIMILAR:
            //     return SimilarContract.getColumnFromIndex(index);
            // case CATEGORIES:
            //     return CategoriesContract.getColumnFromIndex(index);
            case REVIEWS:
                return ReviewsContract.getColumnFromIndex(index);
            default:
                throw new IllegalArgumentException("Table " + table.name().toLowerCase() + " does not exist");
        }
    }

    /**
     * Method to return the number of columns in a table
     *
     * @param table The table whose size is desired
     * @return integer value denoting number of columns in requested table
     * @throws IllegalArgumentException when the table is invalid
     */
    public static int getTableSize(Tables table)
            throws IllegalArgumentException {
        switch (table) {
            case PRODUCTS:
                return ProductsContracts.getNumColumns();
            // case SIMILAR:
            //     return SimilarContract.getNumColumns();
            // case CATEGORIES:
            //     return CategoriesContract.getNumColumns();
            case REVIEWS:
                return ReviewsContract.getNumColumns();
            default:
                throw new IllegalArgumentException("Table " + table.name().toLowerCase() + " does not exist");
        }
    }

    /**
     * Method that returns the name of the csv file corresponding to a given table
     *
     * @param table The table corresponding to which the csv file name is required
     * @return csv file name for the given table
     * @throws IllegalArgumentException when the table name is invalid (highly unlikely)
     */
    public static String getFileName(Tables table) throws IllegalArgumentException {
        switch (table) {
            case PRODUCTS:
                return ProductsContracts.getFileName();
            // case SIMILAR:
            //     return SimilarContract.getFileName();
            // case CATEGORIES:
            //     return CategoriesContract.getFileName();
            case REVIEWS:
                return ReviewsContract.getFileName();
            default:
                throw new IllegalArgumentException("Table " + table.name().toLowerCase() + " does not exist");
        }
    }
    /**
     * Method overridden to ensure that this class is not cloned
     *
     * @return null
     * @throws CloneNotSupportedException since this class cannot be cloned
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("This class cannot be cloned");
    }
}
