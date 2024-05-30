package parser;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import objects.Article;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class ElcProducer {
    private Channel rmqChan = null;
    private final String ARTICLES_QUEUE_NAME;
    private final ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(ElcProducer.class);

    public ElcProducer(Channel channel, String articlesQueueName) {
        rmqChan = channel;
        ARTICLES_QUEUE_NAME = articlesQueueName;
    }

    public void ParsePublishNews(Map<String, Document> docVec) throws InterruptedException, IOException {
        if (docVec.isEmpty()) {
            logger.warn("empty map");
        } else {
            for (Map.Entry<String, Document> entry : docVec.entrySet()) {
                mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
                parseProduceToElk(entry.getKey(), entry.getValue());
            }
            Thread.sleep(500);
        }
    }

    public void parseProduceToElk(String url, Document doc) throws IOException {
        Article article = new Article();
        try {
            article.SetHeader(doc.select("div [class=article__title]").getFirst().text());
            article.SetBody(doc.select("div [class=article__body]").getFirst().text());
            article.SetAuthor(doc.select("li [class=article__author-text-link]").getFirst().text());
            DateTimeFormatter f = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
            String s = doc.select("time").getFirst().attr("datetime");

            String timeZoneOffset = s.substring(s.length() - 5);
            String formattedDateTimeString = s.substring(0, s.length() - 5) + timeZoneOffset;
            DateTime dateTime = f.parseDateTime(formattedDateTimeString);
            DateTimeFormatter formatter = ISODateTimeFormat.dateTime();
            String iso8601String = formatter.print(dateTime);
            article.SetDate(iso8601String);

            article.SetURL(url);
            article.SetId();

            rmqChan.basicPublish("", ARTICLES_QUEUE_NAME, null, mapper.writeValueAsBytes(article));
        } catch (Exception e) {
            logger.error(String.valueOf(e));
        }
    }
}
