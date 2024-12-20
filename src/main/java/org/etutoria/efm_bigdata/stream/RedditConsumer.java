package org.etutoria.efm_bigdata.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.etutoria.efm_bigdata.model.RedditPost;
import org.etutoria.efm_bigdata.producer.RedditProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.UUID;

@Service
public class RedditConsumer {
    private final WebClient webClient;
    private final RedditProducer redditProducer;
    private String accessToken;

    @Value("${reddit.api.url}")
    private String redditApiUrl;

    @Value("${reddit.api.client-id}")
    private String redditClientId;

    @Value("${reddit.api.secret}")
    private String redditApiSecret;

    @Value("${reddit.api.token-url}")
    private String redditTokenUrl;

    public RedditConsumer(WebClient.Builder webClientBuilder, RedditProducer redditProducer) {
        ConnectionProvider provider = ConnectionProvider.builder("custom")
                .maxConnections(100)//permet de définir le nombre maximum de connexions
                .pendingAcquireMaxCount(1000)//permet de définir le nombre maximum de connexions en attente
                .pendingAcquireTimeout(Duration.ofSeconds(60))//permet de définir le temps d'attente maximum
                .build();

        this.webClient = webClientBuilder
                .clientConnector(new ReactorClientHttpConnector(HttpClient.create(provider)))
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024)) // 10 MB buffer size
                .build();
        this.redditProducer = redditProducer;
    }


public Mono<Void> searchAndPublish(String movieTitle) {
    String searchId = UUID.randomUUID().toString();
    System.out.println("Starting search with ID: " + searchId + " for movie title: " + movieTitle);

    return getAccessToken()
            .flatMapMany(token -> fetchPosts(token, movieTitle, searchId))
            .flatMap(response -> extractPostData(response, searchId, movieTitle))
            .doOnNext(post -> System.out.println("Search ID: " + searchId + ", Received post: " + post))
            .onErrorResume(WebClientResponseException.class, e -> {
                System.out.println("Error response from Reddit API: " + e.getResponseBodyAsString());
                return Flux.empty();
            })
            .doOnNext(post -> {
                System.out.println("About to publish post to Kafka: " + post);
                redditProducer.sendMessageReddit(post);
                System.out.println("Published post to Kafka: " + post);
            })
            .doFinally(signalType -> System.out.println("Search ID: " + searchId + ", Search completed with signal: " + signalType))
            .then();
}

    private Mono<String> getAccessToken() {
        return webClient.post()
                .uri(redditTokenUrl)
                .headers(headers -> headers.setBasicAuth(redditClientId, redditApiSecret))
                .body(BodyInserters.fromFormData("grant_type", "client_credentials"))
                .retrieve()
                .bodyToMono(String.class)
                .map(response -> {
                    String accessToken = extractAccessToken(response);
                   // System.out.println("Access token: " + accessToken);
                    this.accessToken = accessToken;
                    return accessToken;
                });
    }

    private Flux<JsonNode> fetchPosts(String accessToken, String query, String searchId) {
        System.out.println("Search ID: " + searchId + ", Fetching posts for query: " + query);
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .scheme("https")
                        .host("oauth.reddit.com")
                        .path("/r/movies/search")
                        .queryParam("q", query)
                        .queryParam("sort", "new")
                        .queryParam("limit", "100")
                        .queryParam("restrict_sr", "true")
                        .build())
                .header("Authorization", "Bearer " + accessToken)
                .retrieve()
                .bodyToFlux(String.class)
                .map(this::parseJson)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(5)));
    }

    private JsonNode parseJson(String response) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readTree(response);
        } catch (Exception e) {
            System.out.println("Error parsing JSON response: " + e.getMessage());
            return null;
        }
    }


private Flux<String> extractPostData(JsonNode response, String searchId, String searchTitle) {
    return Flux.fromIterable(response.get("data").get("children"))
            .flatMap(postNode -> {
                JsonNode postData = postNode.get("data");
                RedditPost post = new RedditPost();
                post.setTitle(postData.get("title").asText());
                post.setAuthor(postData.get("author").asText());
                post.setScore(postData.get("score").asInt());
                post.setNumComments(postData.get("num_comments").asInt());
                post.setUrl(postData.get("url").asText());
                post.setContent(postData.get("selftext").asText());
                post.setSearchTitle(searchTitle);
                return fetchComments(postData.get("id").asText(), searchId)
                        .map(comments -> {
                            post.setComments(comments);
                            return post;
                        });
            })
            .map(this::convertPostToJson);
}

    private Mono<String> fetchComments(String postId, String searchId) {
        System.out.println("Search ID: " + searchId + ", Fetching comments for post ID: " + postId);
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .scheme("https")
                        .host("oauth.reddit.com")
                        .path("/comments/" + postId)
                        .build())
                .header("Authorization", "Bearer " + accessToken)
                .retrieve()
                .bodyToMono(String.class)
                .map(this::parseComments);
    }

    private String parseComments(String response) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(response);
            StringBuilder comments = new StringBuilder();
            JsonNode commentsArray = jsonNode.get(1).get("data").get("children");
            if (commentsArray != null) {
                for (JsonNode commentNode : commentsArray) {
                    JsonNode commentData = commentNode.get("data");
                    if (commentData != null && commentData.get("body") != null) {
                        comments.append(commentData.get("body").asText()).append(" | ");
                    }
                }
            }
            return comments.toString();
        } catch (Exception e) {
            System.out.println("Error parsing comments JSON response: " + e.getMessage());
            return "";
        }
    }

    private String extractAccessToken(String response) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(response);
            return jsonNode.get("access_token").asText();
        } catch (Exception e) {
            System.out.println("Error extracting access token from response: " + e.getMessage());
            return null;
        }
    }


private String convertPostToJson(RedditPost post) {
    try {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode postJson = objectMapper.createObjectNode();
        postJson.put("searchTitle", "Voici le résultat de recherche pour ce Titre de film : " + post.getSearchTitle());
        postJson.put("title", post.getTitle());
        postJson.put("author", post.getAuthor());
        postJson.put("score", post.getScore());
        postJson.put("numComments", post.getNumComments());
        postJson.put("url", post.getUrl());
        postJson.put("comments", post.getComments());
        postJson.put("content", post.getContent());
        return objectMapper.writeValueAsString(postJson);
    } catch (Exception e) {
        System.out.println("Error converting post to JSON: " + e.getMessage());
        return "";
    }
}
}