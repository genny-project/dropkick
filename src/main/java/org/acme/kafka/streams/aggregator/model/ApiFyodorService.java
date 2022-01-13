package org.acme.kafka.streams.aggregator.model;

import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;


@Path("/")
@RegisterRestClient
public interface ApiFyodorService {
   
    @POST
    @Path("/api/search")
    @Produces("application/json")
    String getSearchResults(final String searchBE, @HeaderParam("Authorization") final String bearertoken);
    

   
}
