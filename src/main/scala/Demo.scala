import java.io.{File, IOException, PrintWriter}
import com.orientechnologies.orient.core.metadata.schema.OType
import com.tinkerpop.blueprints.impls.orient._
import com.tinkerpop.blueprints.{Edge, Parameter, Vertex}
import java.util.concurrent.TimeUnit
import scala.io.Source

object Demo{
  //val url: String = "C:\\Users\\Thinh\\Downloads\\DBLPOnlyCitationOct19.txt" bản hơn triệu nút
  val url: String = "C:\\Users\\Thinh\\Downloads\\outputacm.txt" // bản hơn 600 ngàn nút
  val url2: String = "C:\\Users\\Thinh\\Downloads\\authors.txt" // tác giả
  val PAPER_TITLE_PREFIX: String = "#*"
  val PAPER_AUTHORS_PREFIX: String = "#@"
  val PAPER_YEAR_PREFIX: String = "#t"
  val PAPER_VENUE_PREFIX: String = "#c"
  val REFERRED_INDEXED_ID_PREFIX: String = "#%"
  val PAPER_INDEXED_ID_PREFIX: String = "#index"
  val ABSTRACT_INDEXED_PREFIX: String = "#!"

  val orientDbProtocol: String = "remote"
  //val orientDbHost: String = "192.168.1.100" // may ao thưc hien nap du lieu co phan tan
  //val orientDbHost: String = "192.168.1.101"  // may ao thưc hien nap du lieu khong phan tan
  val orientDbHost: String = "localhost"   // may that
  val orientDbDatabase: String = "dblp3"
  val orientDbUsername: String = "root"
  //val orientDbPassword: String = "123abc"    // may ao
  val orientDbPassword: String = "123456" // may that
  var count: Int = 0
  var countAuthor: Int = 1

  def prepareGraph(graph: OrientGraph): Unit ={
      val paper: OrientVertexType = graph.createVertexType("PAPER")
      paper.createProperty("id",OType.LONG)
      paper.createProperty("title",OType.STRING)
      paper.createProperty("author",OType.STRING)
      paper.createProperty("year",OType.STRING)
      paper.createProperty("venue",OType.STRING)
      paper.createProperty("index",OType.STRING)
      paper.createProperty("abstract",OType.STRING)
      paper.createProperty("references",OType.STRING)

      graph.createKeyIndex("index",classOf[Vertex],new Parameter("class","PAPER"))

      val reference: OrientEdgeType = graph.createEdgeType("REFERENCE")
      reference.createProperty("src",OType.LONG)
      reference.createProperty("dst",OType.LONG)

      val author:OrientVertexType = graph.createVertexType("AUTHOR")
      author.createProperty("id",OType.LONG)
      author.createProperty("authorName",OType.STRING)

      graph.createKeyIndex("indexAuthor",classOf[Vertex],new Parameter("class","AUTHOR"))

      val authorOF: OrientEdgeType = graph.createEdgeType("AUTHOR_OF")
      authorOF.createProperty("auThor",OType.LONG)
      authorOF.createProperty("auThorName",OType.LONG)
  }

  def importNode(graph: OrientGraph,filename:String): Unit ={
    val source = Source.fromFile(filename)
    var paper: Vertex = null
    for(line <- source.getLines()){
      if(line.contains(PAPER_TITLE_PREFIX)){
        count +=1
        println(count)
        paper = graph.addVertex("class:PAPER",Nil:_*)
        paper.setProperty("id",count)
        paper.setProperty("title",line.substring(2).trim)
        if(count % 100 == 0) graph.commit()
      }
      else if(line.contains(PAPER_AUTHORS_PREFIX))
        paper.setProperty("author",line.substring(2).trim)
      else if(line.contains(PAPER_YEAR_PREFIX))
        paper.setProperty("year",line.substring(2).trim)
      else if(line.contains(PAPER_VENUE_PREFIX))
        paper.setProperty("venue",line.substring(2).trim)
      else if(line.contains(PAPER_INDEXED_ID_PREFIX))
        paper.setProperty("index",line.substring(6).trim)
      else if(line.contains(ABSTRACT_INDEXED_PREFIX))
        paper.setProperty("abstract",line.substring(2).trim)
      else if(line.contains(REFERRED_INDEXED_ID_PREFIX)){
        var ref : String = paper.getProperty[String]("references")
        if(ref == null){
          ref = ""
        }
        ref += line.substring(2).trim + ";"
        paper.setProperty("references",ref)
      }
    }
  }

  def importNodeToAuthor(graph: OrientGraph,filename:String): Unit ={
    val source = Source.fromFile(filename)
    var author: Vertex = null
    for(line <- source.getLines()){
      if(line != null){
        countAuthor +=1
        println(countAuthor)
        author = graph.addVertex("class:AUTHOR",Nil:_*)
        author.setProperty("id",countAuthor)
        val kt = line.split(",")
        kt.foreach(name =>{
            author.setProperty("authorName",name)
        })
        if(countAuthor % 100 == 0) graph.commit()
      }
    }
  }

  def createEdge(graph: OrientGraph): Unit ={
    var paper:Vertex= null
    val Iterator = graph.getVertices().iterator()
    while (Iterator.hasNext){
      paper=Iterator.next()
      if(paper.getProperty("references")!=null){
        val indices = paper.getProperty("references").toString.split(";")
        indices.foreach(refIndex =>{
          val innerIterator = graph.getVertices("PAPER.index",refIndex).iterator()
          if(innerIterator.hasNext){
            val originalPaper: Vertex = innerIterator.next()
            val reference : Edge = graph.addEdge(null,paper,originalPaper,"REFERENCE")
            reference.setProperty("src",paper.getProperty("id"))
            reference.setProperty("dst",originalPaper.getProperty("id"))
            count+=1
            if(count %100==0) graph.commit()
          }
        })
      }
    }
  }

  def createEdgeAuthorPaper(graph: OrientGraph): Unit ={
    var paper:Vertex= null
    val Iterator = graph.getVertices().iterator()
    while (Iterator.hasNext){
      paper=Iterator.next()
      if(paper.getProperty("author")!=null){
        val indices = paper.getProperty("author").toString.split(",")
        indices.foreach(refIndex =>{
          val innerIterator = graph.getVertices("authorName",refIndex).iterator()
          if(innerIterator.hasNext){
            val Author: Vertex = innerIterator.next()
            val reference : Edge = graph.addEdge(null,paper,Author,"AUTHOR_OF")
            reference.setProperty("auThor",paper.getProperty("author"))
            reference.setProperty("auThorName",Author.getProperty("authorName"))
            count+=1
            if(count %100==0) graph.commit()
          }
        })
      }
    }
  }



  def main(args: Array[String]): Unit ={
    val start = System.nanoTime() // bắt đầu tính thời gian
    println("Begin")
    val orientDbUri: String = s"${orientDbProtocol}:${orientDbHost}/${orientDbDatabase}"
    val graph: OrientGraph = new OrientGraph(orientDbUri, orientDbUsername, orientDbPassword)
    graph.setStandardElementConstraints(false)
    try{
      prepareGraph(graph)
      importNode(graph,url)
      createEdge(graph)
      importNodeToAuthor(graph,url2)
      createEdgeAuthorPaper(graph)
    }catch {
      //In ra lỗi nếu có
      case ex: Exception => println(ex.getMessage)
    }
    finally {
      //Đóng kết nối với OrientDB
      println("Kết thúc....")
      graph.shutdown()
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
