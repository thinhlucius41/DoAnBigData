import com.orientechnologies.orient.core.metadata.schema.OType
import com.tinkerpop.blueprints.impls.orient._
import com.tinkerpop.blueprints.Vertex
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
object ImportData {

  //val url: String = "C:\\Users\\Thinh\\Downloads\\DBLPOnlyCitationOct19.txt" //bản hơn triệu nút
  val url: String = "C:\\Users\\Thinh\\Downloads\\outputacm.txt" // bản hơn 600 ngàn nút

  val PAPER_TITLE_PREFIX: String = "#*"
  val PAPER_AUTHORS_PREFIX: String = "#@"
  val PAPER_YEAR_PREFIX: String = "#t"
  val PAPER_VENUE_PREFIX: String = "#c"
  val REFERRED_INDEXED_ID_PREFIX: String = "#%"
  val PAPER_INDEXED_ID_PREFIX: String = "#index"
  val ABSTRACT_INDEXED_PREFIX: String = "#!"

  val orientDbProtocol: String = "remote"
  val orientDbHost: String = "192.168.1.100" // may ao thưc hien nap du lieu co phan tan
  //val orientDbHost: String = "192.168.1.101"  // may ao thưc hien nap du lieu khong phan tan
  //val orientDbHost: String = "localhost"   // may that
  val orientDbDatabase: String = "dblp3"
  val orientDbUsername: String = "root"
  val orientDbPassword: String = "123abc"    // may ao
  //val orientDbPassword: String = "123456" // may that

  def main(args: Array[String]): Unit = {
    val start = System.nanoTime() // bắt đầu tính thời gian
    val sourceFile = Source.fromFile(url)

    var paperTitle: String = null
    var paperAUTHOR: String = null
    var paperAuthors: ListBuffer[String] = ListBuffer()
    var paperIndexId: String = null
    var paperYear: String = null
    var paperVenue: String = null
    var paperAbstract: String = null
    var paperReferences: ListBuffer[String] = ListBuffer()

    var PaperIdTitleMap = mutable.HashMap[String, String]()
    var PaperIdYearMap = mutable.HashMap[String, String]()
    var PaperIdVenueMap = mutable.HashMap[String, String]()
    var PaperIdAbstractMap = mutable.HashMap[String, String]()
    var PaperIdAUTHORSMap = mutable.HashMap[String, String]()

    var AuthorNameIdMap = mutable.HashMap[String, String]()
    var PaperIdAuthorIdMap = mutable.HashMap[String, String]()
    var PaperIdReferredIdMap = mutable.HashMap[String, String]()
    val AuthorIdReferredIdMap = mutable.HashMap[String, String]()
    var currentAuthorIdCount = 1

    sourceFile.getLines().foreach(line => {

      if (line.startsWith(PAPER_INDEXED_ID_PREFIX)) {
        paperIndexId = line.replace(PAPER_INDEXED_ID_PREFIX, "")
        //Lấy thông tin bài báo dựa trên key index và gán thông tin đó báo hashmap
        if (paperIndexId != null && paperTitle != null) {

          PaperIdTitleMap += (paperIndexId -> paperTitle)
          PaperIdYearMap  += (paperIndexId-> paperYear)
          PaperIdVenueMap += (paperIndexId->paperVenue)
          PaperIdAbstractMap += (paperIndexId->paperAbstract)
          PaperIdAUTHORSMap += (paperIndexId->paperAUTHOR)

          if (paperAuthors.nonEmpty) {
            paperAuthors.foreach(authorName => {
              if (!AuthorNameIdMap.contains(authorName)) {
                AuthorNameIdMap += (authorName -> currentAuthorIdCount.toString)
                PaperIdAuthorIdMap += (paperIndexId -> currentAuthorIdCount.toString)
                currentAuthorIdCount += 1
              } else {
                PaperIdAuthorIdMap += (paperIndexId -> AuthorNameIdMap(authorName))
              }
            })
          }

          if (paperReferences.nonEmpty) {
            paperReferences.foreach(referredPaperId => {
              PaperIdReferredIdMap += (paperIndexId -> referredPaperId)
            })
          }

          //Xóa dữ liệu cũ
          paperTitle = null
          paperIndexId = null
          paperAuthors = ListBuffer()
          paperReferences = ListBuffer()
        }
      } else if (line.startsWith(PAPER_TITLE_PREFIX))
        paperTitle = line.replace(PAPER_TITLE_PREFIX, "")
      else if (line.startsWith(PAPER_AUTHORS_PREFIX)) {
        paperAUTHOR = line.replace(PAPER_AUTHORS_PREFIX, "")
        if(paperAUTHOR != null){
          val authors = paperAUTHOR.split(',')
          if (authors.length > 0)
            authors.foreach(author => paperAuthors += author)
        }
      }
      else if(line.contains(PAPER_YEAR_PREFIX))
        paperYear = line.replace(PAPER_YEAR_PREFIX,"")
      else if(line.contains(PAPER_VENUE_PREFIX))
        paperVenue= line.replace(PAPER_VENUE_PREFIX,"")
      else if(line.contains(ABSTRACT_INDEXED_PREFIX))
        paperAbstract= line.replace(ABSTRACT_INDEXED_PREFIX,"")
      else if (line.startsWith(REFERRED_INDEXED_ID_PREFIX)) {
        val referredIndexedPaperId = line.replace(REFERRED_INDEXED_ID_PREFIX, "")
        paperReferences += referredIndexedPaperId
      }
    })

//    println("Tổng số bài báo: " + PaperIdTitleMap.keySet.count(z => true))
//    println("Tổng số tác giả: " + AuthorNameIdMap.keySet.count(z => true))
//    println("Nạp dữ liệu vào OrientDb...")

    //Thiết lập cớ sở dữ liệu để lưu trữ
    val orientDbUri: String = s"$orientDbProtocol:$orientDbHost/$orientDbDatabase"
    val dblpOrientDbGraph: OrientGraph = new OrientGraph(orientDbUri, orientDbUsername, orientDbPassword)

    var PaperIdVertexIdMap = mutable.HashMap[String, AnyRef]()
    var AuthorIdVertexIdMap = mutable.HashMap[String, AnyRef]()

    try {

      if (dblpOrientDbGraph.getVertexType("Paper") == null) {
        val person: OrientVertexType = dblpOrientDbGraph.createVertexType("Paper")
        person.createProperty("title", OType.STRING)
        person.createProperty("author",OType.STRING)
        person.createProperty("year",OType.STRING)
        person.createProperty("venue",OType.STRING)
        person.createProperty("abstract",OType.STRING)
        person.createProperty("indexedId", OType.STRING)
      }

      if (dblpOrientDbGraph.getVertexType("Author") == null) {
        val person: OrientVertexType = dblpOrientDbGraph.createVertexType("Author")
        person.createProperty("name", OType.STRING)
        person.createProperty("indexedId", OType.STRING)
      }

      var count = 0

      //Clear all current data first
      /*graph.command(new OCommandSQL("DELETE VERTEX V")).execute()
      graph.command(new OCommandSQL("DELETE EDGE E")).execute()*/
        //Thêm đỉnh paper
        PaperIdTitleMap.keySet.foreach(paperIndexId => {
            val targetPaperVertex: Vertex = dblpOrientDbGraph.addVertex("class:Paper", Nil: _*)
            targetPaperVertex.setProperty("indexedId", paperIndexId)
            targetPaperVertex.setProperty("title", PaperIdTitleMap(paperIndexId))
            targetPaperVertex.setProperty("year", PaperIdYearMap.get(paperIndexId))
            targetPaperVertex.setProperty("venue", PaperIdVenueMap.get(paperIndexId))
            targetPaperVertex.setProperty("author", PaperIdAUTHORSMap.get(paperIndexId))
            targetPaperVertex.setProperty("abstract", PaperIdAbstractMap.get(paperIndexId))
            PaperIdVertexIdMap += (paperIndexId -> targetPaperVertex.getId)
            if (count % 1000 == 0) dblpOrientDbGraph.commit()
            count += 1
        })

        //Thêm đỉnh author
        AuthorNameIdMap.keySet.foreach(authorName => {
            val targetAuthorVertex: Vertex = dblpOrientDbGraph.addVertex("class:Author", Nil: _*)
            targetAuthorVertex.setProperty("indexedId", AuthorNameIdMap(authorName))
            targetAuthorVertex.setProperty("name", authorName)
            AuthorIdVertexIdMap += (AuthorNameIdMap(authorName) -> targetAuthorVertex.getId)
            if (count % 1000 == 0) dblpOrientDbGraph.commit()
            count += 1
        })


      //Thêm cạnh giửa author-paper
      PaperIdAuthorIdMap.keySet.foreach(paperIndexId => {
        val targetPaperVertex = dblpOrientDbGraph.getVertex(PaperIdVertexIdMap(paperIndexId))
        val targetAuthorVertex = dblpOrientDbGraph.getVertex(AuthorIdVertexIdMap(PaperIdAuthorIdMap(paperIndexId)))
        if (targetPaperVertex != null && targetAuthorVertex != null) {
          dblpOrientDbGraph.addEdge(null, targetAuthorVertex, targetPaperVertex, "author_of")
          if (count % 1000 == 0) dblpOrientDbGraph.commit()
          count += 1
        }
      })

      //Thêm cạnh giửa paper-paper
      PaperIdReferredIdMap.keySet.foreach(paperIndexId => {
        val targetPaperVertex = dblpOrientDbGraph.getVertex(PaperIdVertexIdMap(paperIndexId))
        val targetReferredPaperVertex = dblpOrientDbGraph.getVertex(PaperIdVertexIdMap(PaperIdReferredIdMap(paperIndexId)))
        if (targetPaperVertex != null && targetReferredPaperVertex != null) {
          dblpOrientDbGraph.addEdge(null, targetPaperVertex, targetReferredPaperVertex, "reference")
          if (count % 1000 == 0) dblpOrientDbGraph.commit()
          count += 1
        }
      })

      //Thêm cạnh giửa author-author
      AuthorIdReferredIdMap.keySet.foreach(authorIndexId => {
        val targetAuthorVertex = dblpOrientDbGraph.getVertex(AuthorIdVertexIdMap(PaperIdAuthorIdMap(authorIndexId)))
        val targetReferredAuthorVertex = dblpOrientDbGraph.getVertex(AuthorIdVertexIdMap(AuthorIdReferredIdMap(PaperIdAuthorIdMap(authorIndexId))))
        if (targetAuthorVertex != null && targetReferredAuthorVertex != null) {
          dblpOrientDbGraph.addEdge(null, targetAuthorVertex, targetReferredAuthorVertex, "has_relatived")
          if (count % 1000 == 0) dblpOrientDbGraph.commit()
          count += 1
        }
      })

    } catch {
      //Bắt lỗi
      case ex: Exception => println(ex.getMessage)
    }
    finally {
      //Tắt kết nối với OrientDB
      dblpOrientDbGraph.shutdown()
    }
    val end = System.nanoTime() // kết thức tính thời gian
    val difference = end - start // tính thời gian thực hiện nạp dữ liệu
    println("Tổng thời gian thực hiện: " +
      TimeUnit.NANOSECONDS.toHours(difference) + " giờ " +
      ( TimeUnit.NANOSECONDS.toMinutes(difference) -  TimeUnit.HOURS.toMinutes(TimeUnit.NANOSECONDS.toHours(difference)))   + " phút " +
      ( TimeUnit.NANOSECONDS.toSeconds(difference) -  TimeUnit.MINUTES.toSeconds(TimeUnit.NANOSECONDS.toMinutes(difference))) + " giây " +
      " - " + difference + " giây (Tổng thời gian)")
  }
}
