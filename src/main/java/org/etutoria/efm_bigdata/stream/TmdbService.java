package org.etutoria.efm_bigdata.stream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.etutoria.efm_bigdata.producer.TmdbProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.List;

@Service
@Slf4j
public class TmdbService {
    private final WebClient webClient;
    private final TmdbProducer tmdbProducer;
    private final RedditConsumer redditConsumer;

    public TmdbService(WebClient.Builder webClientBuilder, TmdbProducer tmdbProducer, RedditConsumer redditConsumer) {
        this.webClient = webClientBuilder
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024)) // 10 MB buffer size
                .build();
        this.tmdbProducer = tmdbProducer;
        this.redditConsumer = redditConsumer;
    }

    @Value("${tmdb.api.token}")
    private String apiToken;

    @Value("${tmdb.api.endpoints.genreList}")
    private String genreListEndpoint;

    @Value("${tmdb.api.endpoints.popularMovies}")
    private String popularMoviesEndpoint;

    @Value("${tmdb.api.endpoints.movieDetails}")
    private String movieDetailsEndpoint;

    public void consumeAndPublishAll(List<Integer> movieIds) {
        consumeAndPublish(genreListEndpoint, "Genre List").subscribe();//le subscribe permet de déclencher la méthode consumeAndPublish
        consumeAndPublish(popularMoviesEndpoint, "Popular Movies")
                .flatMapMany(this::extractMovieTitles)//.flatMapMany permet de transformer le flux en un autre flux
                .flatMap(redditConsumer::searchAndPublish)
                .subscribe();
        for (int movieId : movieIds) {
            consumeAndPublish(movieDetailsEndpoint.replace("{movie_id}", String.valueOf(movieId)), "Movie Details").subscribe();
        }
    }

    public Mono<String> consumeAndPublish(String endpoint, String dataType) {//cette méthode permet de consommer les données de l'API TMDB et de les publier sur le topic tmdballtest
        return fetchEndpointData(endpoint, dataType)
                .doOnTerminate(() -> System.out.println("Finished consuming " + dataType + " from endpoint: " + endpoint));//affiche un message une fois que la méthode est terminée
    }

    private Mono<String> fetchEndpointData(String endpoint, String dataType) {
        return webClient.get()
                .uri(endpoint)
                .headers(headers -> headers.setBearerAuth(apiToken))
                .retrieve()
                .bodyToMono(String.class)//récupère le body de la réponse
                .flatMap(response -> {//.flatMap permet de transformer la réponse en flux
                    System.out.println("----- Start of " + dataType + " Data -----");
                    tmdbProducer.sendMessageTmdb(response);
                    System.out.println("----- End of " + dataType + " Data -----");
                    return Mono.just(response);//retourne la réponse sous forme de Mono cad un flux avec un seul élément
                })
                .onErrorResume(error -> {
                    System.out.println("Error consuming " + dataType + " from endpoint: " + endpoint);
                    return Mono.empty();
                });
    }

    private Flux<String> extractMovieTitles(String response) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();//permet de lire le json de la réponse
            JsonNode jsonNode = objectMapper.readTree(response);//récupère le json de la réponse cad le body
            return Flux.fromIterable(jsonNode.get("results"))
                    .map(node -> node.get("title").asText());
        } catch (Exception e) {
            System.out.println("Error extracting movie titles from response: " + response);
            return Flux.empty();
        }
    }
}