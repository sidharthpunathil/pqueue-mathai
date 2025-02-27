import * as fs from 'fs';
import * as path from 'path';
import axios from 'axios';
import * as crypto from 'crypto';
import PQueue from 'p-queue';

interface Question {
    question: string;
    timestamp: Date | string;
    success?: boolean;
    mcq_type?: string;
    hash?: string;
}

interface QuestionCategory {
    numerical?: Question[];
    symbolic?: Question[];
    statement?: Question[];
    [key: string]: Question[] | undefined;
}

interface QuestionsData {
    easy_questions?: QuestionCategory;
    medium_questions?: QuestionCategory;
    hard_questions?: QuestionCategory;
    [key: string]: QuestionCategory | undefined;
}

interface Solution {
    question: string;
    solution: any;
    mcq_type: string;
    timestamp: string;
    hash?: string;
}

// Store solutions by difficulty and category
interface ConsolidatedSolutions {
    [difficultyAndCategory: string]: Solution[];
}

const jsonFilePath = './introduction-to-trigonometry.json';
const jsonFileCopyPath = './introduction-to-trigonometry_copy.json';
const failedQuestionsPath = './failed_questions.json';
const baseUrl = 'http://localhost:8000';
const solveQuestionRoute = '/solve-question';
const outputBaseDir = './output';
const LLM_PROVIDER = "together";
const CONCURRENCY = 10;

// Required folder and file structure
const requiredFolders = ['easy', 'med', 'hard'];
const requiredFiles = ['numerical.json', 'statement.json', 'symbolic.json'];

// Keep track of consolidated solutions
const consolidatedSolutions: ConsolidatedSolutions = {};

// Create queue with concurrency limit
const queue = new PQueue({ concurrency: CONCURRENCY });

// Generate hash for a question
function generateQuestionHash(question: string): string {
    return crypto.createHash('md5').update(question).digest('hex');
}

async function solveQuestion(question: string, mcqType: string): Promise<{ success: boolean; data: any }> {
    try {
        console.log(`Solving question with MCQ type: ${mcqType}`);
        const response = await axios.post(`${baseUrl}${solveQuestionRoute}`, { 
            question, 
            provider: LLM_PROVIDER, 
            mcq_type: mcqType 
        });
        return { success: response.status === 200, data: response.data };
    } catch (error: any) {
        console.error(`Error solving question: ${question}`, error?.response?.data || error.message);
        return { success: false, data: error };
    }
}

function createSafeDate(timestamp: Date | string): Date {
    if (timestamp instanceof Date) {
        return isNaN(timestamp.getTime()) ? new Date() : timestamp;
    }
    
    try {
        const date = new Date(timestamp);
        return isNaN(date.getTime()) ? new Date() : date;
    } catch (error) {
        console.error(`Invalid timestamp: ${timestamp}, using current time`);
        return new Date();
    }
}

function ensureDirectoryExists(dirPath: string): void {
    try {
        if (!fs.existsSync(dirPath)) {
            fs.mkdirSync(dirPath, { recursive: true });
        }
    } catch (error: any) {
        console.error(`Error creating directory ${dirPath}: ${error.message}`);
    }
}

function normalizeDifficultyName(difficulty: string): string {
    // Convert easy_questions to easy, medium_questions to med, hard_questions to hard
    if (difficulty === 'easy_questions') return 'easy';
    if (difficulty === 'medium_questions') return 'med';
    if (difficulty === 'hard_questions') return 'hard';
    return difficulty;
}

function normalizeCategory(category: string): string {
    // Ensure category is one of numerical, symbolic, or statement
    if (['numerical', 'symbolic', 'statement'].includes(category)) {
        return category;
    }
    return category;
}

async function processQuestion(question: Question, difficulty: string, categoryName: string): Promise<void> {
    try {
        // Add hash if not already present
        if (!question.hash) {
            question.hash = generateQuestionHash(question.question);
        }
        
        const mcqType = categoryName;
        
        const result = await solveQuestion(question.question, mcqType);
        question.success = result.success;
        question.mcq_type = mcqType;

        const timestamp = createSafeDate(question.timestamp);
        
        if (result.success) {
            // Add to consolidated solutions
            const normalizedDifficulty = normalizeDifficultyName(difficulty);
            const normalizedCategory = normalizeCategory(categoryName);
            const key = `${normalizedDifficulty}_${normalizedCategory}`;
            
            if (!consolidatedSolutions[key]) {
                consolidatedSolutions[key] = [];
            }
            
            consolidatedSolutions[key].push({
                question: question.question,
                solution: result.data,
                mcq_type: mcqType,
                timestamp: timestamp.toISOString(),
                hash: question.hash
            });
            
            // Save consolidated JSON after each successful solution
            saveConsolidatedSolutions();
        } else {
            await saveFailedQuestion(difficulty, categoryName, question);
            // No longer deleting failed questions from the copy file
        }

        await updateQuestionStatus(difficulty, categoryName, question);
    } catch (error: any) {
        console.error(`Error processing question: ${question.question}`, error);
        question.success = false;
        await updateQuestionStatus(difficulty, categoryName, question);
    }
}

// Save all consolidated solutions to their respective files
function saveConsolidatedSolutions(): void {
    ensureDirectoryExists(outputBaseDir);
    
    // First, ensure all required folders and empty JSON files exist
    createRequiredStructure();
    
    // Then save actual solutions
    for (const key in consolidatedSolutions) {
        if (Object.prototype.hasOwnProperty.call(consolidatedSolutions, key)) {
            const [difficulty, category] = key.split('_');
            
            // Skip invalid combinations
            if (!requiredFolders.includes(difficulty) || !requiredFiles.includes(`${category}.json`)) {
                continue;
            }
            
            const dirPath = path.join(outputBaseDir, difficulty);
            ensureDirectoryExists(dirPath);
            
            const filePath = path.join(dirPath, `${category}.json`);
            
            try {
                fs.writeFileSync(filePath, JSON.stringify(consolidatedSolutions[key], null, 2));
                console.log(`Successfully saved consolidated solutions to: ${filePath}`);
            } catch (writeError: any) {
                console.error(`Error writing consolidated file ${filePath}: ${writeError.message}`);
            }
        }
    }
}

// Create the required folder and file structure
function createRequiredStructure(): void {
    ensureDirectoryExists(outputBaseDir);
    
    for (const folder of requiredFolders) {
        const folderPath = path.join(outputBaseDir, folder);
        ensureDirectoryExists(folderPath);
        
        for (const file of requiredFiles) {
            const filePath = path.join(folderPath, file);
            
            if (!fs.existsSync(filePath)) {
                try {
                    // Initialize with empty array if doesn't exist
                    fs.writeFileSync(filePath, JSON.stringify([], null, 2));
                    console.log(`Created empty file: ${filePath}`);
                } catch (error: any) {
                    console.error(`Error creating file ${filePath}: ${error.message}`);
                }
            }
        }
    }
}

async function processCategory(questions: Question[], difficulty: string, categoryName: string): Promise<void> {
    console.log(`Processing category: ${difficulty} - ${categoryName}, found ${questions.length} questions`);
    
    // Add all questions to the queue
    const promises = questions.map(question => {
        return queue.add(() => processQuestion(question, difficulty, categoryName));
    });
    
    // Wait for all tasks to complete
    await Promise.all(promises);
}

async function processDifficultyLevel(difficulty: string, questions: QuestionCategory | undefined): Promise<void> {
    if (!questions) {
        console.log(`No questions found for difficulty: ${difficulty}`);
        return;
    }

    const categoryNames = Object.keys(questions);
    console.log(`Found categories for ${difficulty}: ${categoryNames.join(', ')}`);

    for (const categoryName of categoryNames) {
        const categoryQuestions = questions[categoryName];
        if (categoryQuestions && Array.isArray(categoryQuestions)) {
            await processCategory(categoryQuestions, difficulty, categoryName);
        }
    }
}

async function updateQuestionStatus(difficulty: string, categoryName: string, questionToUpdate: Question): Promise<void> {
    try {
        const jsonData = fs.readFileSync(jsonFilePath, 'utf-8');
        const questionsData: QuestionsData = JSON.parse(jsonData);

        const difficultyQuestions = questionsData[difficulty];

        if (difficultyQuestions) {
            const categoryQuestions = difficultyQuestions[categoryName];

            if (categoryQuestions) {
                // Use hash for comparison if available
                const questionIndex = categoryQuestions.findIndex(question => {
                    if (questionToUpdate.hash && question.hash) {
                        return question.hash === questionToUpdate.hash;
                    }
                    
                    const timestamp1 = createSafeDate(question.timestamp);
                    const timestamp2 = createSafeDate(questionToUpdate.timestamp);
                    return question.question === questionToUpdate.question &&
                           timestamp1.getTime() === timestamp2.getTime();
                });

                if (questionIndex > -1) {
                    categoryQuestions[questionIndex].success = questionToUpdate.success;
                    // Add hash if it doesn't exist
                    if (!categoryQuestions[questionIndex].hash) {
                        categoryQuestions[questionIndex].hash = questionToUpdate.hash || generateQuestionHash(questionToUpdate.question);
                    }
                    fs.writeFileSync(jsonFilePath, JSON.stringify(questionsData, null, 2));
                }
            }
        }
    } catch (error: any) {
        console.error(`Error updating question status`, error);
    }
}

async function saveFailedQuestion(difficulty: string, categoryName: string, failedQuestion: Question): Promise<void> {
    try {
        let failedQuestions: { [key: string]: any[] } = {};

        if (fs.existsSync(failedQuestionsPath)) {
            const failedQuestionsData = fs.readFileSync(failedQuestionsPath, 'utf-8');
            failedQuestions = JSON.parse(failedQuestionsData);
        }

        const normalizedDifficulty = normalizeDifficultyName(difficulty);
        const normalizedCategory = normalizeCategory(categoryName);
        const key = `${normalizedDifficulty}-${normalizedCategory}`;

        if (!failedQuestions[key]) {
            failedQuestions[key] = [];
        }
        
        // Check if this question is already in the failed questions list using hash
        const hash = failedQuestion.hash || generateQuestionHash(failedQuestion.question);
        const isDuplicate = failedQuestions[key].some(q => 
            (q.hash && q.hash === hash) || 
            (q.question === failedQuestion.question && new Date(q.timestamp).getTime() === createSafeDate(failedQuestion.timestamp).getTime())
        );
        
        if (!isDuplicate) {
            failedQuestions[key].push({
                ...failedQuestion,
                hash: hash,
                mcq_type: categoryName,
                timestamp: createSafeDate(failedQuestion.timestamp).toISOString()
            });

            fs.writeFileSync(failedQuestionsPath, JSON.stringify(failedQuestions, null, 2));
        }
    } catch (error: any) {
        console.error(`Error saving failed question`, error);
    }
}

async function loadFailedQuestions(): Promise<{ [key: string]: any[] }> {
    try {
        if (fs.existsSync(failedQuestionsPath)) {
            const failedQuestionsData = fs.readFileSync(failedQuestionsPath, 'utf-8');
            return JSON.parse(failedQuestionsData);
        } else {
            return {};
        }
    } catch (error: any) {
        console.error(`Error loading failed questions`, error);
        return {};
    }
}

async function retryFailedQuestions(): Promise<void> {
    const failedQuestions = await loadFailedQuestions();
    
    // Create a queue for retrying failed questions
    const retryQueue = new PQueue({ concurrency: CONCURRENCY });
    const retryPromises = [];

    for (const key in failedQuestions) {
        const [difficulty, categoryName] = key.split('-');
        const questions = failedQuestions[key];

        if (questions && questions.length > 0) {
            for (const question of questions) {
                retryPromises.push(retryQueue.add(async () => {
                    try {
                        const mcqType = question.mcq_type || categoryName;
                        
                        const result = await solveQuestion(question.question, mcqType);
                        question.success = result.success;

                        if (result.success) {
                            failedQuestions[key] = failedQuestions[key].filter((q: any) => {
                                // Use hash for comparison if available
                                if (question.hash && q.hash) {
                                    return q.hash !== question.hash;
                                }
                                return q.question !== question.question || 
                                       new Date(q.timestamp).getTime() !== new Date(question.timestamp).getTime();
                            });
                            
                            const timestamp = createSafeDate(question.timestamp);
                            
                            // Add to consolidated solutions
                            const normalizedDifficulty = normalizeDifficultyName(difficulty);
                            const normalizedCategory = normalizeCategory(categoryName);
                            const solutionKey = `${normalizedDifficulty}_${normalizedCategory}`;
                            
                            if (!consolidatedSolutions[solutionKey]) {
                                consolidatedSolutions[solutionKey] = [];
                            }
                            
                            // Check for duplicates using hash
                            const hash = question.hash || generateQuestionHash(question.question);
                            const isDuplicate = consolidatedSolutions[solutionKey].some(s => 
                                (s.hash && s.hash === hash) || 
                                (s.question === question.question && new Date(s.timestamp).getTime() === timestamp.getTime())
                            );
                            
                            if (!isDuplicate) {
                                consolidatedSolutions[solutionKey].push({
                                    question: question.question,
                                    solution: result.data,
                                    mcq_type: mcqType,
                                    timestamp: timestamp.toISOString(),
                                    hash: hash
                                });
                            }
                            
                            // Save consolidated solutions
                            saveConsolidatedSolutions();
                        }

                        await updateQuestionStatus(difficulty, categoryName, question);
                    } catch (error: any) {
                        console.error(`Failed to retry question: ${question.question}. Error: ${error?.response?.data || error.message}`);
                        question.success = false;
                        await updateQuestionStatus(difficulty, categoryName, question);
                    }
                }));
            }
        }
    }
    
    // Wait for all retry operations to complete
    await Promise.all(retryPromises);

    // Clean up empty categories in failed questions
    try {
        fs.writeFileSync(failedQuestionsPath, JSON.stringify(failedQuestions, null, 2));
        for (const key in failedQuestions) {
            if (failedQuestions[key].length === 0) {
                delete failedQuestions[key];
            }
        }
        fs.writeFileSync(failedQuestionsPath, JSON.stringify(failedQuestions, null, 2));
    } catch (error: any) {
        console.error(`Error updating failed questions file: ${error.message}`);
    }
}

// Load any existing consolidated solutions
function loadExistingConsolidatedSolutions(): void {
    if (!fs.existsSync(outputBaseDir)) {
        return;
    }

    const difficulties = fs.readdirSync(outputBaseDir);
    
    for (const difficulty of difficulties) {
        if (!requiredFolders.includes(difficulty)) {
            continue; // Skip directories that aren't in our required list
        }
        
        const difficultyPath = path.join(outputBaseDir, difficulty);
        
        if (fs.statSync(difficultyPath).isDirectory()) {
            const categoryFiles = fs.readdirSync(difficultyPath);
            
            for (const file of categoryFiles) {
                if (file.endsWith('.json') && requiredFiles.includes(file)) {
                    const categoryName = file.replace('.json', '');
                    const key = `${difficulty}_${categoryName}`;
                    
                    try {
                        const filePath = path.join(difficultyPath, file);
                        const fileData = fs.readFileSync(filePath, 'utf-8');
                        consolidatedSolutions[key] = JSON.parse(fileData);
                        
                        // Add hash to existing solutions if missing
                        for (const solution of consolidatedSolutions[key]) {
                            if (!solution.hash) {
                                solution.hash = generateQuestionHash(solution.question);
                            }
                        }
                        
                        console.log(`Loaded existing solutions for ${key}: ${consolidatedSolutions[key].length} items`);
                    } catch (error: any) {
                        console.error(`Error loading existing solutions for ${key}: ${error.message}`);
                        consolidatedSolutions[key] = [];
                    }
                }
            }
        }
    }
}

// Map results from processing difficulty levels to the required output structure
function mapResultsToRequiredStructure(): void {
    const newSolutions: ConsolidatedSolutions = {};
    
    // Initialize all required combinations with empty arrays
    for (const folder of requiredFolders) {
        for (const file of requiredFiles) {
            const category = file.replace('.json', '');
            newSolutions[`${folder}_${category}`] = [];
        }
    }
    
    // Copy existing solutions to the normalized structure
    for (const key in consolidatedSolutions) {
        const [oldDifficulty, oldCategory] = key.split('_');
        const normalizedDifficulty = normalizeDifficultyName(oldDifficulty);
        const normalizedCategory = normalizeCategory(oldCategory);
        
        // Check if this is a valid combination in our required structure
        if (requiredFolders.includes(normalizedDifficulty) && 
            requiredFiles.includes(`${normalizedCategory}.json`)) {
            
            const newKey = `${normalizedDifficulty}_${normalizedCategory}`;
            
            if (!newSolutions[newKey]) {
                newSolutions[newKey] = [];
            }
            
            // Add solutions, avoiding duplicates using hash
            for (const solution of consolidatedSolutions[key]) {
                const hash = solution.hash || generateQuestionHash(solution.question);
                
                const isDuplicate = newSolutions[newKey].some(s => 
                    (s.hash && s.hash === hash) || 
                    (s.question === solution.question && 
                     new Date(s.timestamp).getTime() === new Date(solution.timestamp).getTime())
                );
                
                if (!isDuplicate) {
                    newSolutions[newKey].push({
                        ...solution,
                        hash: hash
                    });
                }
            }
        }
    }
    
    // Replace the old solutions with the new structure
    Object.assign(consolidatedSolutions, newSolutions);
}

// Check if copy file exists to resume processing
function shouldResumeFromCopy(): boolean {
    return fs.existsSync(jsonFileCopyPath);
}

// Add hash to all questions in the data
function addHashesToQuestions(questionsData: QuestionsData): QuestionsData {
    for (const difficulty in questionsData) {
        const difficultyObj = questionsData[difficulty];
        if (difficultyObj) {
            for (const category in difficultyObj) {
                const questions = difficultyObj[category];
                if (questions && Array.isArray(questions)) {
                    for (const question of questions) {
                        if (!question.hash) {
                            question.hash = generateQuestionHash(question.question);
                        }
                    }
                }
            }
        }
    }
    return questionsData;
}

async function main(): Promise<void> {
    try {
        // Create required directory structure at the beginning
        createRequiredStructure();
        
        // Check if we should resume from a copy file
        if (shouldResumeFromCopy()) {
            console.log("Found existing copy file. Resuming from previous state...");
        } else {
            // If no copy file exists, make sure the original JSON exists
            if (!fs.existsSync(jsonFilePath)) {
                fs.writeFileSync(jsonFilePath, JSON.stringify({ 
                    "easy_questions": {
                        "numerical": [],
                        "symbolic": [],
                        "statement": []
                    }, 
                    "medium_questions": {
                        "numerical": [],
                        "symbolic": [],
                        "statement": []
                    }, 
                    "hard_questions": {
                        "numerical": [],
                        "symbolic": [],
                        "statement": []
                    } 
                }, null, 2));
            }

            // Create a copy of the original JSON file for processing
            fs.copyFileSync(jsonFilePath, jsonFileCopyPath);
        }
        
        // Load any existing consolidated solutions before processing
        loadExistingConsolidatedSolutions();

        // Load and process data from the copy file
        const jsonData = fs.readFileSync(jsonFileCopyPath, 'utf-8');
        let questionsData: QuestionsData = JSON.parse(jsonData);
        
        // Add hashes to all questions for better identification
        questionsData = addHashesToQuestions(questionsData);
        fs.writeFileSync(jsonFileCopyPath, JSON.stringify(questionsData, null, 2));

        // Get all difficulty levels from the JSON
        const difficultyLevels = Object.keys(questionsData);
        console.log(`Found difficulty levels: ${difficultyLevels.join(', ')}`);

        // Process each difficulty level sequentially
        for (const difficulty of difficultyLevels) {
            await processDifficultyLevel(difficulty, questionsData[difficulty]);
        }
        
        // Retry any questions that failed in previous runs
        await retryFailedQuestions();

        // Map results to required structure
        mapResultsToRequiredStructure();
        
        // Save all consolidated solutions at the end
        saveConsolidatedSolutions();

        console.log("Processing completed successfully");
        
        // Log queue statistics
        console.log(`Queue statistics:
          - Size: ${queue.size}
          - Pending: ${queue.pending}
          - Concurrency: ${queue.concurrency}
        `);
    } catch (error: any) {
        console.error(`Error in main execution: ${error.message}`);
    }
}

main();