import java.io.{File, PrintWriter}

import com.orientechnologies.orient.core.sql.OCommandSQL
import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.{OrientDynaElementIterable, OrientGraph, OrientVertex}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.IOException
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object SparkGraphXPageRank {
  val orientDbProtocol: String = "remote"
  val orientDbHost: String = "localhost"
  val orientDbDatabase: String = "dblp2"
  val orientDbUsername: String = "root"
  val orientDbPassword: String = "123456"
  val numberOfTakenPapers: Int = 2000

  def main(args: Array[String]): Unit ={
    //Thiết lập môi trường OrientDB
    val orientDbUri: String = s"$orientDbProtocol:$orientDbHost/$orientDbDatabase"
    val dblpOrientDbGraph: OrientGraph = new OrientGraph(orientDbUri, orientDbUsername, orientDbPassword)

    //Thiết lập môi trường Spark
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SparkGraphX").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")

    var addedPapers = mutable.HashMap[String, String]()
    var dblpVertices = ArrayBuffer[(VertexId, String)]()
    var dblpEdges = ArrayBuffer[Edge[String]]()

    try {

      //Lấy các đỉnh Paper
      val results: OrientDynaElementIterable = dblpOrientDbGraph
        .command(new OCommandSQL(s"SELECT FROM PAPER LIMIT ${numberOfTakenPapers.toString}"))
        .execute()

      results.forEach(v => {
        //Lặp qua từng đỉnh Paper
        val paper = v.asInstanceOf[OrientVertex]
        if (paper.getProperty("id") != null) {
          val paperIndexedId = paper.getProperty("id").toString
          val paperTitle = paper.getProperty("title").toString
          dblpVertices += ((paperIndexedId.toLong, paperTitle))
          addedPapers += (paperIndexedId -> paperTitle)
          val paperReferencesIterator = paper.getEdges(Direction.IN, "reference").iterator()

          //Lấy cạnh từ đỉnh Paper đang được trỏ tới
          while (paperReferencesIterator.hasNext) {
            val referredPaper = paperReferencesIterator.next().getVertex(Direction.OUT).asInstanceOf[OrientVertex]
            if (referredPaper.getProperty("id") != null ) {
              val referredPaperIndexedId = referredPaper.getProperty("id").toString
              val referredPaperTitle = referredPaper.getProperty("title").toString
              if (!addedPapers.keySet.contains(referredPaperIndexedId)) {
                dblpVertices += ((referredPaperIndexedId.toLong, referredPaperTitle))
                addedPapers += (referredPaperIndexedId -> referredPaperTitle)
              }
              dblpEdges += Edge(paperIndexedId.toLong, referredPaperIndexedId.toLong, "reference")
            }
          }
        }
      })

      //println(s"Reading total: [${dblpVertices.count(z => true)}] vertices and [${dblpEdges.count(z => true)}] edges from OrientDb !")

      //Tạo GraphX từ những đỉnh và cạnh của DBLP
      val rddVertices: RDD[(VertexId, String)] = sparkContext.parallelize(dblpVertices)
      val rddEdgs: RDD[Edge[String]] = sparkContext.parallelize(dblpEdges)
      val dblpGraphX = Graph(rddVertices, rddEdgs)

      //Thiết lập tính PageRank
      val tolConstant = 0.0001
      val rankedVertices = dblpGraphX.pageRank(tolConstant).vertices
      val cc = dblpGraphX.connectedComponents().vertices

      //Lấy top 10 và in ra file text ketqua.txt
      /*val top10RankedPapers = dblpGraphX.vertices.join(rankedVertices).sortBy(rankedPaper => rankedPaper._2._2, false).take(10)
      top10RankedPapers.foreach(rankedPaper => {
        println(s" - Paper: [${rankedPaper._1}][${rankedPaper._2._1}] - score: [${rankedPaper._2._2}]")
      })*/

      //Bước 1: Tạo đối tượng luồng fos và liên kết nguồn dữ liệu
      val fos = new PrintWriter(new File("C:\\Users\\Thinh\\Downloads\\ketqua.txt"))
      fos.write(s"Đọc tất cả: [${dblpVertices.count(z => true)}] đỉnh and [${dblpEdges.count(z => true)}] cạnh từ OrientDb !\n\n")
      val top10RankedPapers = dblpGraphX.vertices.join(rankedVertices).sortBy(rankedPaper => rankedPaper._2._2, ascending = false).take(10)
      top10RankedPapers.foreach(rankedPaper => {
        try {
          //Bước 2: Ghi dữ liệu rankedPaper kiểu dữ liệu (id,(tên bài báo,ranked bài báo))
          fos.write(s" - Bài báo: [${rankedPaper._1}][${rankedPaper._2._1}] - ranked: [${rankedPaper._2._2}] \n")
        } catch {
          case ex: IOException =>
            ex.printStackTrace()
        }
      })
      //Bước 3: Đóng luồng PageRank
      fos.close()
      System.out.println("PageRank Done!")

      //Bước 4: Tạo đối tượng luồng fos1 và liên kết nguồn dữ liệu
      val fos1 = new PrintWriter(new File("C:\\Users\\Thinh\\Downloads\\usernames.txt"))
      val ConectComponents = dblpGraphX.vertices.join(cc).take(2000)
      ConectComponents.foreach(paper => {
        try {
          //Bước 5: Ghi dữ liệu paper kiểu dữ liệu (id,(tên bài báo,bài báo thuộc Connect Componet nào))
          fos1.write(s"[${paper._1}][${paper._2._1}][${paper._2._2}] \n")
        } catch {
          case ex: IOException =>
            ex.printStackTrace()
        }
      })
      //Bước 6: Đóng luồng ConnectComponent
      fos1.close()
      System.out.println("ConnectComponent Done!")
    } catch {
      //In ra lỗi
      case ex: Exception => println(ex.getMessage)
    }
    finally {
      //Đóng kết nối với OrientDB
      dblpOrientDbGraph.shutdown()
    }
  }
}
