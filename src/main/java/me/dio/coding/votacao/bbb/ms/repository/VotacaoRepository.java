package me.dio.coding.votacao.bbb.ms.repository;

import org.springframework.data.mongodb.repository.MongoRepository;

import me.dio.coding.votacao.bbb.ms.model.VotacaoModel;

public interface VotacaoRepository extends MongoRepository<VotacaoModel, String>{

}
