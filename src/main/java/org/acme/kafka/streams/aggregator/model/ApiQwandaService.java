package org.acme.kafka.streams.aggregator.model;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;


@Path("/qwanda")
@RegisterRestClient
public interface ApiQwandaService {
   
    @POST
    @Path("/baseentitys/search25")
    @Produces("application/json")
    String getSearchResults(final String searchBE, @HeaderParam("Authorization") final String bearertoken);
    
    @GET
    @Path("/attributes")
    @Produces("application/json")
    String getAttributes(@HeaderParam("Authorization") final String bearertoken);

    @GET
    @Path("/baseentitys/{code}")
    @Produces("application/json")
    String getBaseEntity(@PathParam("code") final String code,@HeaderParam("Authorization") final String bearertoken);

    @POST
    @Path("/baseentitys")
    @Produces("application/json")
    String createBaseEntity(final String baseEntityJson,@HeaderParam("Authorization") final String bearertoken);

    @POST
    @Path("/entityentitys")
    @Produces("application/json")
    String createEntityEntity(final String entityEntityJson,@HeaderParam("Authorization") final String bearertoken);
   
}
