package com.amex.lumi.ingestion.exception;

import com.amex.lumi.ingestion.dto.ApiErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.client.RestClientException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

@RestControllerAdvice
public class GlobalExceptionHandler {

	private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

	@ExceptionHandler(MethodArgumentNotValidException.class)
	public ResponseEntity<ApiErrorResponse> handleValidation(
			MethodArgumentNotValidException exception,
			HttpServletRequest request) {
		List<String> details = exception.getBindingResult().getAllErrors().stream()
				.map(error -> error.getDefaultMessage() == null ? "Invalid value" : error.getDefaultMessage())
				.toList();
		return errorResponse(HttpStatus.BAD_REQUEST, "Request validation failed", request, details);
	}

	@ExceptionHandler(HttpMessageNotReadableException.class)
	public ResponseEntity<ApiErrorResponse> handleUnreadableRequest(HttpServletRequest request) {
		return errorResponse(HttpStatus.BAD_REQUEST, "Request body is missing or malformed", request, List.of());
	}

	@ExceptionHandler(FileNotFoundException.class)
	public ResponseEntity<ApiErrorResponse> handleFileNotFound(HttpServletRequest request) {
		return errorResponse(HttpStatus.NOT_FOUND, "Input file was not found", request, List.of());
	}

	@ExceptionHandler(IllegalArgumentException.class)
	public ResponseEntity<ApiErrorResponse> handleInvalidArgument(
			IllegalArgumentException exception,
			HttpServletRequest request) {
		return errorResponse(HttpStatus.BAD_REQUEST, exception.getMessage(), request, List.of());
	}

	@ExceptionHandler(RestClientException.class)
	public ResponseEntity<ApiErrorResponse> handleAirflowFailure(HttpServletRequest request) {
		return errorResponse(HttpStatus.BAD_GATEWAY, "Unable to communicate with Airflow", request, List.of());
	}

	@ExceptionHandler(IOException.class)
	public ResponseEntity<ApiErrorResponse> handleFileProcessingFailure(HttpServletRequest request) {
		return errorResponse(HttpStatus.INTERNAL_SERVER_ERROR,
				"An error occurred while processing the input file", request, List.of());
	}

	@ExceptionHandler(Exception.class)
	public ResponseEntity<ApiErrorResponse> handleUnexpectedFailure(
			Exception exception,
			HttpServletRequest request) {
		logger.error("Unhandled exception while processing {}", request.getRequestURI(), exception);
		return errorResponse(HttpStatus.INTERNAL_SERVER_ERROR,
				"An unexpected error occurred", request, List.of());
	}

	private ResponseEntity<ApiErrorResponse> errorResponse(
			HttpStatus status,
			String message,
			HttpServletRequest request,
			List<String> details) {
		ApiErrorResponse body = new ApiErrorResponse(
				Instant.now(),
				status.value(),
				status.getReasonPhrase(),
				message,
				request.getRequestURI(),
				details
		);
		return ResponseEntity.status(status).body(body);
	}
}
