package com.danielgmyers.flux.clients.swf.spring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Should be attached to a (String) bean identifying the SWF endpoint which should be used to contact SWF.
 * To be used with FluxSpringCreator.
 */
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Qualifier
public @interface WorkflowEndpoint {
}
