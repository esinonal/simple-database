package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

    /**
     * A helper class that defines a table. It has a name, pkeyField, and DbFile.
     * */
    public static class UserTable {

        private String pkeyField;
        private String name;
        private DbFile file;
        private int id;

        public UserTable(DbFile f, String n, String p) {
            this.pkeyField = p;
            this.name = n;
            this.file = f;
            this.id = f.getId();
        }

        public String getPkeyField() { return this.pkeyField; }
        public String getName() { return this.name; }
        public DbFile getFile() { return this.file; }
        public int getId() { return this.id; }

        public void setName(String n) { this.name = n;}
        public void setFile(DbFile f) { this.file = f;}
    }

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    private ConcurrentHashMap<String, UserTable> nameHash;
    private ConcurrentHashMap<Integer, String> idHash;

    public Catalog() {
        nameHash = new ConcurrentHashMap<>();
        idHash = new ConcurrentHashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        if (name != null) {
            // Use hashtable so its the last table added for that name
            String potentialConflictingFileId = idHash.get(file.getId());
            if (potentialConflictingFileId != null) {
                // means weve found file with the same id
                nameHash.remove(potentialConflictingFileId); //we'll replace it in the next line
            }
            UserTable potentialConflictingFileName = nameHash.get(name);
            if (potentialConflictingFileName != null) {
                int conflictingId = potentialConflictingFileName.getFile().getId();
                idHash.remove(conflictingId);
            }
            nameHash.put(name, new UserTable(file, name, pkeyField));
            idHash.put(file.getId(), name);
        } else {
            throw new NullPointerException();
        }
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        Set<Integer> keys = idHash.keySet();
        for (Integer key: keys) {
            if (idHash.get(key).equals(name)) {
                return key;
            }
        }
        String exception = "No table with given name:" + name;
        throw new NoSuchElementException(exception);
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        String name = idHash.get(tableid);
        if (name != null) {
            return nameHash.get(name).getFile().getTupleDesc();
        }
        String exception = "No table with given id:" + tableid;
        throw new NoSuchElementException(exception);
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        String name = idHash.get(tableid);
        if (name != null) {
            return nameHash.get(name).getFile();
        }
        String exception = "No table with given id:" + tableid;
        throw new NoSuchElementException(exception);
    }

    public String getPrimaryKey(int tableid) {
        String name = idHash.get(tableid);
        if (name != null) {
            return nameHash.get(name).getPkeyField();
        }
        return null;
    }

    public Iterator<Integer> tableIdIterator() {
        ArrayList<Integer> al = new ArrayList<Integer>();
        Set<Integer> keys = idHash.keySet();
        for (Integer key: keys) {
            al.add(key);
        }
        return al.iterator();
    }

    /** get table name given id **/
    public String getTableName(int id)  {
        String name = idHash.get(id);
        if (name != null) {
            return nameHash.get(name).getName();
        }
        return null;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        nameHash.clear();
        idHash.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

