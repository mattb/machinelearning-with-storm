import java.io.BufferedReader
import java.io.PrintWriter
import java.lang.Process
import java.lang.ProcessBuilder
import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.{Fields, Tuple, Values}
import collection.mutable.{Map, HashMap}
import com.aliasi.lm.TokenizedLM
import com.aliasi.tokenizer.EnglishStopTokenizerFactory
import com.aliasi.tokenizer.IndoEuropeanTokenizerFactory
import com.aliasi.tokenizer.TokenizerFactory
import com.aliasi.util.AbstractExternalizable
import com.aliasi.util.Files
import com.aliasi.util.ScoredObject
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import edu.stanford.nlp.ling._
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.util._
import org.apache.commons.lang3.StringEscapeUtils
import org.jsoup._
import org.jsoup.safety._
import redis.clients.jedis.Jedis
import scala.collection.JavaConverters._
import storm.scala.dsl._
import yieldbot.storm.spout.RedisPubSubSpout
import com.google.common.cache._

case class Example(id: String, tokens: List[String], tags: Set[String])

object Example {
  val mapper = {
    val m = new ObjectMapper()
    m.getJsonFactory.disable(JsonFactory.Feature.INTERN_FIELD_NAMES).disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
    m
  }

  def htmlToText(html: String) = StringEscapeUtils.unescapeHtml4(Jsoup.clean(html, Whitelist.none))

  def fromJson(json: String) = {
    val data = mapper.readTree(json)
    Example(data.path("id").textValue,
                List(htmlToText(data.path("text").textValue)),
                data.path("tags").elements.asScala.map { tag => tag.textValue }.toSet)
  }
}

class ParseJson extends StormBolt(outputFields = List("example")) {
  def execute(t: Tuple) = t matchSeq {
    case Seq(json: String) => using anchor t emit Example.fromJson(json)
    case _ => { // 
    }
    t ack
  }
}

class LemmatiseExample extends StormBolt(outputFields = List("example")) {
  val punctuation = Set(".",",","``","''",":","(",")","#","$","'","\"", "-LRB-", "-RRB-")
  var stopwords: Set[String] = _
  var nlp: StanfordCoreNLP = _

  setup {
    stopwords = io.Source.fromInputStream(getClass.getResourceAsStream("stopwords.txt")).getLines.toSet
    nlp = {
      val config = new java.util.Properties
      config.put("annotators", "tokenize, ssplit, pos, lemma")
      new StanfordCoreNLP(config)
    }
  }

  def lemmatise(text: String) = {
    val annotation = new Annotation(text)
    nlp.annotate(annotation)
    annotation.get[java.util.List[CoreMap], CoreAnnotations.SentencesAnnotation](classOf[CoreAnnotations.SentencesAnnotation]).asScala.flatMap { sentence =>
      sentence.get[java.util.List[CoreLabel], CoreAnnotations.TokensAnnotation](classOf[CoreAnnotations.TokensAnnotation]).asScala.filter { token =>
        val pos = token.get[String, CoreAnnotations.PartOfSpeechAnnotation](classOf[CoreAnnotations.PartOfSpeechAnnotation])

        !punctuation.contains(pos) && 
        !token.lemma.startsWith("<") &&
        !stopwords.contains(token.lemma.toLowerCase) &&
        token.lemma.length > 2
      }.map { token =>
        token.lemma.toLowerCase
      }
    }.toList
  }

  def execute(t: Tuple) = t matchSeq {
    case Seq(example: Example) => using anchor t emit example.copy(tokens=lemmatise(example.tokens.head))
    case _ => { // tick
    }
    t ack
  }
}

class Collocations extends StormBolt(outputFields = List()) {
  var r: Jedis = _
  val tokenizerFactory = IndoEuropeanTokenizerFactory.INSTANCE
  var model: TokenizedLM = _
  var count = 0

  setup { 
    r = new Jedis("127.0.0.1", 6379) 
    model = new TokenizedLM(tokenizerFactory, 2)
  }

  def flushModel = {
    model.collocationSet(2, 8, 10000).asScala.filter { ngram =>
      ngram.score > 10000 && ngram.getObject.forall(_.size > 1)
    }.foreach { ngram =>
      r.hset("collocations", ngram.getObject.mkString(" ").toLowerCase, ngram.score.toString)
    }
    model = new TokenizedLM(tokenizerFactory, 2)
    count = 0
  }

  def execute(t: Tuple) = t matchSeq {
    case Seq(example: Example) => {
      count += 1
      model.handle(example.tokens.mkString(" "))
      if(count == 1000) {
        flushModel
      }
    }
    case _ => { // tick
    }
    t ack
  }
}

class Collocise extends StormBolt(outputFields = List("example")) {
  var collocations = Set[String]()
  var r: Jedis = _
  setup { 
    r = new Jedis("127.0.0.1", 6379) 
    loadCollocations
  }

  def loadCollocations = {
    collocations = r.hgetAll("collocations").keySet.asScala.toSet
  }

  def collocise(words: List[String], joinWith: String = " ") = {
    var tokens = List[String]()
    val token = words.iterator.buffered
    while(token.hasNext) {
      val word = token.next
      if(token.hasNext && collocations.contains(word + " " + token.head)) {
        tokens = (word + joinWith + token.next) :: tokens
      } else {
        tokens = word :: tokens
      }
    }
    tokens.reverse
  }

  def execute(t: Tuple) = t matchSeq {
    case Seq(example: Example) => using anchor t emit example.copy(tokens=collocise(example.tokens))
    case _ => { 
      loadCollocations
    }
    t ack
  }
}

class RemoveRareWords extends StormBolt(outputFields = List("example")) {
  var rarewords: LoadingCache[String, java.lang.Boolean] = _
  var r: Jedis = _
  setup { 
    r = new Jedis("127.0.0.1", 6379) 
    rarewords = CacheBuilder.newBuilder
      .expireAfterWrite(60, java.util.concurrent.TimeUnit.SECONDS)
      .maximumSize(1000000)
      .build(new CacheLoader[String, java.lang.Boolean] {
        override def load(word: String): java.lang.Boolean = {
          val score = r.zscore("frequency:term", word)
          if(score == null) {
            true
          } else if(score > 5) {
            false
          } else {
            true
          }
        }
      })
  }

  def remove_rarewords(words: List[String]) = {
    words.filter { word => !rarewords.get(word) }
  }

  def execute(t: Tuple) = t matchSeq {
    case Seq(example: Example) => using anchor t emit example.copy(tokens=remove_rarewords(example.tokens))
    case _ => { 
      //tick
    }
    t ack
  }
}

class TermFrequency extends StormBolt(outputFields = List()) {
  var r: Jedis = _
  setup { r = new Jedis("127.0.0.1", 6379) }
  def execute(t: Tuple) = t matchSeq {
    case Seq(example: Example) => example.tokens.groupBy(t => t).foreach { case(t, ts) => 
      r.zincrby("frequency:term", ts.size, t)
    }
    case _ => { // tick
    }
    t ack
  }
}

class VWExampleEncoder extends StormBolt(outputFields = List("vwdata", "example")) {
  var r: Jedis = _
  setup { r = new Jedis("127.0.0.1", 6379) }
  def execute(t: Tuple) = t matchSeq {
    case Seq(example: Example) => {
      if(example.tokens.size > 10) {
        val encoded = example.tokens.groupBy(token => token).map {
          case (t, ts) => t + ":" + ts.size
        }
        val output = List("'" + example.id + "|") ++ encoded
        r.publish("vw", output.mkString(" "))
        using anchor t emit (output.mkString(" "), example)
      }
    }
    case _ => { // tick
    }
    t ack
  }
}

class VWClassifierTrainer extends StormBolt(outputFields = List("example", "prediction")) {
  var vw: Process = _
  var fromVW: BufferedReader = _
  var toVW: PrintWriter = _
  var exampleCount = 0
  setup { startNewModel }

  def startNewModel = {
    var options = List("-b", "20", "--loss_function", "logistic")
    val filename = "politics.dat"

    if(vw != null) {
      vw.getOutputStream.close
      vw.waitFor
      options = options ++ List("-i", filename)
    }

    val builder = new ProcessBuilder((List("vw", "--quiet", "-f", filename) ++ options).asJava)
    vw = builder.start
    fromVW = new BufferedReader(new java.io.InputStreamReader(vw.getInputStream))
    toVW = new PrintWriter(vw.getOutputStream, true)
  }

  def execute(t: Tuple) = t matchSeq {
    case Seq(encodedExample: String, example: Example) => {
      exampleCount += 1
      val label = if(example.tags.exists(e => e.equals("politics/politics"))) {
        "1"
      } else {
        "-1"
      }
      toVW.println(label + " " + encodedExample)
      if(exampleCount % 2000 == 0) {
        startNewModel
      }
    }
    case _ => { // tick
    }
    t ack
  }
}

object WabbitTopology {
  def main(args: Array[String]) = {
    val builder = new TopologyBuilder

    builder.setSpout("source", new RedisPubSubSpout("127.0.0.1", 6379, "text"), 1)
    builder.setBolt("parse", new ParseJson, 2).shuffleGrouping("source")
    builder.setBolt("lemmatise", new LemmatiseExample, 8).shuffleGrouping("parse")
    builder.setBolt("collocations", new Collocations, 2).shuffleGrouping("lemmatise")
    builder.setBolt("collocise", new Collocise, 2).shuffleGrouping("lemmatise")
    builder.setBolt("termfreq", new TermFrequency, 2).shuffleGrouping("collocise")
    builder.setBolt("rarewords", new RemoveRareWords, 2).shuffleGrouping("collocise")
    builder.setBolt("vwencoder", new VWExampleEncoder, 1).shuffleGrouping("rarewords")
    builder.setBolt("classifier", new VWClassifierTrainer, 1).shuffleGrouping("vwencoder")

    val conf = new Config
    conf.setNumWorkers(20);
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, new java.lang.Integer(60))

    //val cluster = new LocalCluster
    //cluster.submitTopology("learning", conf, builder.createTopology)
    backtype.storm.StormSubmitter.submitTopology("learning", conf, builder.createTopology)
  }
}
