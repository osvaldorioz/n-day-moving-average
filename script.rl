     final int deep = 20;
     final int size = list.size();
     final List<String> fechas = new ArrayList<String>();
     final List<Double> precios = new ArrayList<Double>();
     final List<Double> avgs = new ArrayList<Double>();
     final List<Row> rows = new ArrayList<Row>();

     int ndx = 0;
     
     SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
     
     final List<StructField> fields = new ArrayList<StructField>();
            fields.add(DataTypes.createStructField("ticker", DataTypes.StringType, false));
            fields.add(DataTypes.createStructField("open", DataTypes.DoubleType, false));
            fields.add(DataTypes.createStructField("close", DataTypes.DoubleType, false));
            fields.add(DataTypes.createStructField("adj_close", DataTypes.DoubleType, false));
            fields.add(DataTypes.createStructField("low", DataTypes.DoubleType, false));
            fields.add(DataTypes.createStructField("high", DataTypes.DoubleType, false));
            fields.add(DataTypes.createStructField("volume", DataTypes.LongType, false));
            fields.add(DataTypes.createStructField("date", DataTypes.DateType, false));

     final String emisora = "SPGI";
     a.setQuery("SELECT date, close FROM data where date >= '2015-01-01' and date <= '2015-12-31' and ticker = '"+ emisora + "'");

     final List<Row> list = a.readCSV("/kueski/historical_stock_prices.csv", fields).collectAsList();

     for(Row row: list){
         fechas.add(fmt.format(row.getDate(0)));
         precios.add(row.getDouble(1));
     }

     avgs.add(0.0);

     for(int i = 0; i < size; i++){
         ndx = i + 1;
         Double avg = 0d;
         if(ndx <= size - deep){
             for(int j = 0; j < deep; j++){
                 avg += precios.get(ndx++);
             }
             avg /= deep;
         }
         avgs.add(avg);
     }

     for(int i = 0; i < size; i++){
         rows.add(RowFactory.create(fechas.get(i),precios.get(i),avgs.get(i)));
     }

     System.out.println("\nEmisora: "+emisora+ " Size: "+rows.size());

     fields.clear();
     fields.add(DataTypes.createStructField("fecha", DataTypes.StringType, true));
     fields.add(DataTypes.createStructField("precio", DataTypes.DoubleType, true));
     fields.add(DataTypes.createStructField("moving_average", DataTypes.DoubleType, true));

     a.createCSV("/kueski/output", rows, fields);
