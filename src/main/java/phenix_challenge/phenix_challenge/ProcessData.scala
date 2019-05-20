package phenix_challenge.phenix_challenge

import scala.io.Source
import scala.io.BufferedSource
import scala.collection.immutable.ListMap

import java.io.File
import java.io.PrintWriter

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import java.io.FileNotFoundException
import java.io.IOException

object ProcessData {

	def main(args: Array[String]) {
		val t0 = System.nanoTime()

				// files path
				val path = "C:/Users/mahmoud/Downloads/phenix-challenge-master/phenix-challenge-master/data/"

				//process date
				val date = "20170514"

				//get and process transactions data of "20170514"
				lazy val txRows = getData(path , date, 0)
				treatData(txRows , date , path, "")

				//get and process transactions data from "20170508" to "20170514"
				lazy val txRowsAllDays = getData(path , date, 6)
				treatData(txRowsAllDays , date , path, "-J7")

				val t1 = System.nanoTime()
				println("Elapsed time: " + (t1 - t0) + "ns")

	}

	/**
	 * this function concatenate all transactions files in one iterator
	 * input : (path of files, process date, nbre of day to process)
	 * output : ietrator of all transactions details 
	 */

	def getData(path : String, dateS : String, NbOfDay : Int) : Iterator[Array[String]] = {

			val formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
			val runDay = LocalDate.parse(dateS, formatter);
			var txRowsAllDays = Iterator[Array[String]]()

					for( a <- 0 to NbOfDay){
						val date = runDay.minusDays(a).toString().replace("-", "")

								try {
									val tx = Source.fromFile(path+"transactions_"+date+".data")
											val txRows = tx.getLines().map(line => line.split('|'))
											txRowsAllDays =  txRowsAllDays ++ txRows
								} catch {
								case ex: FileNotFoundException =>{
									println("transactions_"+date+".data not found")
								}

								case ex: IOException => {
									println("IO Exception")
								}
								}
					}

			txRowsAllDays
	}

	/**
	 * this function process data to generate results in files
	 * input : (transactions , process date, path of files , end of files name example -J7)
	 */

	def treatData(txRows: Iterator[Array[String]] ,date:String, path:String, endOfName : String){
	  
	      // this iterable will contain (id_product , revenues, quantity)
		    var productAllMagasin =  Iterable[(String, Float, Int)]() 
		    
		    //*********************************************************************************
		    //********************* process data by id_magasin ********************************
		    //*********************************************************************************

		    // process data grouped by id_magasin
				txRows.toList.groupBy(row => row(2)).foreach(magasin => {
					    val idMagasin = magasin._1
							val iterByMagasin = magasin._2 

							//process data for specific id_magasin grouped by id_product 
							val product = iterByMagasin.filter(row => row(3) != "0").groupBy(row => row(3)).map(product => {
								    val idProduct = product._1 
										val iterByProduct = product._2
										
										//quantity of product 
										var qteByProduct = 0 
										//date of referencial data
										var date = ""

										iterByProduct.map(row => (row(1),row(4))).foreach(tuple => { 
											qteByProduct += tuple._2.toInt
													date = tuple._1.slice(0, 8)
										})

										// get price of specefic product
										val ref = Source.fromFile(path+"reference_prod-"+idMagasin+"_"+date+".data")
										val refRows = ref.getLines().map(line => line.split('|'))
										val price =refRows.filter(rows => rows(0) == idProduct ).map(row => row(1)).toList(0).toFloat

										val CAByProduit = price * qteByProduct
										
										// return (id_product , revenue , quantity)
										(idProduct , CAByProduit, qteByProduct)
							})

							productAllMagasin = productAllMagasin ++ product

							//create top_100_ca_<ID_MAGASIN>_YYYYMMDD.data files
							val caForProduct = product.map(product => (product._1,product._2))

							// sort data by revenue
							val caForProductIter = collection.mutable.LinkedHashMap(caForProduct.toSeq.sortWith(_._2 > _._2):_*).toIterable

							//save top 100 product to file
							saveData(caForProductIter , path+"top_100_ca_"+idMagasin+"_"+date+endOfName+".data")


							//create top_100_ventes_<ID_MAGASIN>_YYYYMMDD.data files 

							val salesForProduct = product.map(product => (product._1,product._3))

							//sort data by quantity
							val salesForProductIter = collection.mutable.LinkedHashMap(salesForProduct.toSeq.sortWith(_._2 > _._2):_*).toIterable

							//save top 100 product to file
							saveData(salesForProductIter , path+"top_100_ventes_"+idMagasin+"_"+date+endOfName+".data")     

				})

				//*********************************************************************************
		    //****************************** Golbal process ***********************************
		    //*********************************************************************************

				// process data grouped by id_product
				val totalProductAllMagasin= productAllMagasin.groupBy(product => product._1).map({ product =>

				val idProduct = product._1
				val iterByProduct = product._2
				//quantity of specefic product in all stores
				var qteByProductTotal = 0 
				//revenues of specefic product in all stores
				var CAByProductTotal = 0 

				iterByProduct.foreach(tuple => {
					CAByProductTotal += tuple._2.toInt
							qteByProductTotal += tuple._3.toInt
				})

				(idProduct , CAByProductTotal, qteByProductTotal)

				})

				//create top_100_ca_GLOBAL_YYYYMMDD.data files

				val caForProductsAllMagasin = totalProductAllMagasin.map(product => (product._1,product._2))

				val caForProductsAllMagasinIter = collection.mutable.LinkedHashMap(caForProductsAllMagasin.toSeq.sortWith(_._2 > _._2):_*).toIterable

				saveData(caForProductsAllMagasinIter , path+"top_100_ca_GLOBAL_"+date+endOfName+".data")


				//create top_100_ventes_GLOBAL_YYYYMMDD.data files 

				val venteForProductsAllMagasin = totalProductAllMagasin.map(product => (product._1,product._3))

				val venteForProductsAllMagasinIter = collection.mutable.LinkedHashMap(venteForProductsAllMagasin.toSeq.sortWith(_._2 > _._2):_*).toIterable

				saveData(venteForProductsAllMagasinIter , path+"top_100_ventes_GLOBAL_"+date+endOfName+".data")

	}

	/**
	 * this function save data to file
	 * input : (iterator of data)
	 */
	def saveData( iter: Iterable[(String, Any)], path:String ){

		val writer = new PrintWriter(new File(path))

		    // save top 100 product to file
				iter.take(100).foreach(produit => writer.write(produit._1+"|"+produit._2+"\r\n"))

				writer.close()

	}
}
