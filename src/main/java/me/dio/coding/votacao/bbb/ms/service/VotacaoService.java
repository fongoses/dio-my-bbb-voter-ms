package me.dio.coding.votacao.bbb.ms.service;

import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.dio.coding.votacao.bbb.ms.model.ParticipanteModel;
import me.dio.coding.votacao.bbb.ms.model.VotacaoModel;
import me.dio.coding.votacao.bbb.ms.repository.VotacaoRepository;

@Service
@Slf4j
@AllArgsConstructor
public class VotacaoService {
	
	private final VotacaoRepository repository;
	
	@KafkaListener(topics = "votacao", groupId = "MicroServicoVotacao")
	private void executar(ConsumerRecord<String, String> registro) {
		
		String participanteStr = registro.value();
		log.info("Voto recebido = {}", participanteStr);
		
		ObjectMapper mapper = new ObjectMapper();
		ParticipanteModel participante = null;

		try {
			participante = mapper.readValue(participanteStr, ParticipanteModel.class);
		} catch (JsonMappingException e) {
			log.error("Erro ao mapear classe [{}]", participanteStr, e);
			return;
		} catch (JsonProcessingException e) {
			log.error("Erro ao converter voto [{}]", participanteStr, e);
			return;
		}
		
		VotacaoModel votacao = new VotacaoModel(null, participante, new Date());
		VotacaoModel entity = repository.save(votacao);
		
		log.info("Voto registrado com sucesso [id={}, datahora={}]", entity.getId(), entity.getDatahora());
		
	}

}
