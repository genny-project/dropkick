package org.acme.kafka.streams.aggregator.model;

import java.util.Set;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;


@Path("/")
@RegisterRestClient
public interface ApiBridgeService {
   
    @PUT
    @Path("/admin/blacklist/{uuid}")
    @Produces("application/json")
    String addBlacklistUUID(@PathParam("uuid") final String uuid, @HeaderParam("Authorization") final String bearertoken);
 
    @DELETE
    @Path("/admin/blacklist/{uuid}")
    @Produces("application/json")
    String removeBlacklistUUID(@PathParam("uuid") final String uuid, @HeaderParam("Authorization") final String bearertoken);

    @GET
    @Path("/admin/blacklists")
    @Produces("application/json")
    Set<String> getBlackLists(@HeaderParam("Authorization") final String bearertoken);

    
}
