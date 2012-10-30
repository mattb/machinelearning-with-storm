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

case class Example(id: String, tokens: List[String], tags: Set[String]) {
  def lemmatise = {
    if(tokens.size > 1) {
      this
    } else {
      this.copy(tokens=Example.lemmatise(tokens.head))
    }
  }
}

object Example {
  val punctuation = Set(".",",","``","''",":","(",")","#","$","'","\"", "-LRB-", "-RRB-")
  lazy val stopwords = io.Source.fromFile(getClass.getResource("stopwords.txt").toURI.getPath).getLines.toSet
  lazy val nlp = {
    val config = new java.util.Properties
    config.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(config)
  }
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
  def execute(t: Tuple) = t matchSeq {
    case Seq(example: Example) => using anchor t emit example.lemmatise
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
    r = new Jedis("localhost", 6379) 
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
    r = new Jedis("localhost", 6379) 
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
      println("Reloading collocations")
      loadCollocations
    }
    t ack
  }
}

class TermFrequency extends StormBolt(outputFields = List()) {
  var r: Jedis = _
  setup { r = new Jedis("localhost", 6379) }
  def execute(t: Tuple) = t matchSeq {
    case Seq(example: Example) => example.tokens.groupBy(t => t).foreach { case(t, ts) => 
      r.zincrby("frequency:term", ts.size, t)
    }
    case _ => { // tick
    }
    t ack
  }
}

object WabbitTopology {
  def main(args: Array[String]) = {
    val builder = new TopologyBuilder

    builder.setSpout("source", new RedisPubSubSpout("localhost", 6379, "text"), 1)
    builder.setBolt("parse", new ParseJson, 3).shuffleGrouping("source")
    builder.setBolt("lemmatise", new LemmatiseExample, 3).shuffleGrouping("parse")
    builder.setBolt("termfreq", new TermFrequency, 3).shuffleGrouping("lemmatise")
    builder.setBolt("collocations", new Collocations, 1).shuffleGrouping("lemmatise")
    builder.setBolt("collocise", new Collocise, 3).shuffleGrouping("lemmatise")

    val conf = new Config
    conf.setDebug(true)
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, new java.lang.Integer(60));

    val cluster = new LocalCluster
    cluster.submitTopology("learning", conf, builder.createTopology)
  }
}
